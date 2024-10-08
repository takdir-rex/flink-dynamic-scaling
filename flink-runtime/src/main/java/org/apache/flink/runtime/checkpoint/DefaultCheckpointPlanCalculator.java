/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for {@link CheckpointPlanCalculator}. If all tasks are running, it
 * directly marks all the sources as tasks to trigger, otherwise it would try to find the running
 * tasks without running processors as tasks to trigger.
 */
public class DefaultCheckpointPlanCalculator implements CheckpointPlanCalculator {

    private final JobID jobId;

    private final CheckpointPlanCalculatorContext context;

    private final List<ExecutionJobVertex> jobVerticesInTopologyOrder = new ArrayList<>();

    private final List<ExecutionVertex> allTasks = new ArrayList<>();

    private final List<ExecutionVertex> sourceTasks = new ArrayList<>();

    private final boolean allowCheckpointsAfterTasksFinished;

    private final Iterable<ExecutionJobVertex> jobVerticesInTopologyOrderIterable;

    public DefaultCheckpointPlanCalculator(
            JobID jobId,
            CheckpointPlanCalculatorContext context,
            Iterable<ExecutionJobVertex> jobVerticesInTopologyOrderIterable,
            boolean allowCheckpointsAfterTasksFinished) {

        this.jobId = checkNotNull(jobId);
        this.context = checkNotNull(context);
        this.allowCheckpointsAfterTasksFinished = allowCheckpointsAfterTasksFinished;

        checkNotNull(jobVerticesInTopologyOrderIterable);
        this.jobVerticesInTopologyOrderIterable = jobVerticesInTopologyOrderIterable;
    }

    @Override
    public CompletableFuture<CheckpointPlan> calculateCheckpointPlan() {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        if (context.hasFinishedTasks() && !allowCheckpointsAfterTasksFinished) {
                            throw new CheckpointException(
                                    "Some tasks of the job have already finished and checkpointing with finished tasks is not enabled.",
                                    CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
                        }

                        checkAllTasksInitiated();

                        CheckpointPlan result =
                                context.hasFinishedTasks()
                                        ? calculateAfterTasksFinished()
                                        : calculateWithAllTasksRunning();

                        checkTasksStarted(result.getTasksToWaitFor());

                        return result;
                    } catch (Throwable throwable) {
                        throw new CompletionException(throwable);
                    }
                },
                context.getMainExecutor());
    }

    @Override
    public CompletableFuture<CheckpointPlan> calculateCheckpointPlan(final String snapshotGroup) {
        final boolean hasFinishedTasks;
        if (snapshotGroup.startsWith("rescale-")) {
            hasFinishedTasks = context.hasFinishedTasks();
        } else {
            hasFinishedTasks = hasFinishedTask(snapshotGroup);
        }
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        if (hasFinishedTasks && !allowCheckpointsAfterTasksFinished) {
                            throw new CheckpointException(
                                    "Some tasks of the job have already finished and checkpointing with finished tasks is not enabled.",
                                    CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
                        }

                        checkAllTasksInitiated();

                        CheckpointPlan result =
                                hasFinishedTasks
                                        ? calculateAfterTasksFinished(snapshotGroup)
                                        : calculateWithAllTasksRunning(snapshotGroup);

                        checkTasksStarted(result.getTasksToWaitFor());

                        return result;
                    } catch (Throwable throwable) {
                        throw new CompletionException(throwable);
                    }
                },
                context.getMainExecutor());
    }

    /**
     * Checks if all tasks are attached with the current Execution already. This method should be
     * called from JobMaster main thread executor.
     *
     * @throws CheckpointException if some tasks do not have attached Execution.
     */
    private void checkAllTasksInitiated() throws CheckpointException {
        jobVerticesInTopologyOrder.clear();
        sourceTasks.clear();
        jobVerticesInTopologyOrderIterable.forEach(
                jobVertex -> {
                    jobVerticesInTopologyOrder.add(jobVertex);
                    allTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));

                    if (jobVertex.getJobVertex().isInputVertex()) {
                        sourceTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                    }
                });
        for (ExecutionVertex task : allTasks) {
            if (task.getCurrentExecutionAttempt() == null) {
                throw new CheckpointException(
                        String.format(
                                "task %s of job %s is not being executed at the moment. Aborting checkpoint.",
                                task.getTaskNameWithSubtaskIndex(), jobId),
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
    }

    /**
     * Checks if all tasks to trigger have already been in RUNNING state. This method should be
     * called from JobMaster main thread executor.
     *
     * @throws CheckpointException if some tasks to trigger have not turned into RUNNING yet.
     */
    private void checkTasksStarted(List<Execution> toTrigger) throws CheckpointException {
        for (Execution execution : toTrigger) {
            if (execution.getState() != ExecutionState.RUNNING) {
                throw new CheckpointException(
                        String.format(
                                "Checkpoint triggering task %s of job %s is not being executed at the moment. "
                                        + "Aborting checkpoint.",
                                execution.getVertex().getTaskNameWithSubtaskIndex(), jobId),
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
    }

    /**
     * Computes the checkpoint plan when all tasks are running. It would simply marks all the source
     * tasks as need to trigger and all the tasks as need to wait and commit.
     *
     * @return The plan of this checkpoint.
     */
    private CheckpointPlan calculateWithAllTasksRunning() {
        List<Execution> executionsToTrigger =
                sourceTasks.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());

        List<Execution> tasksToWaitFor = createTaskToWaitFor(allTasks);

        return new DefaultCheckpointPlan(
                Collections.unmodifiableList(executionsToTrigger),
                Collections.unmodifiableList(tasksToWaitFor),
                Collections.unmodifiableList(allTasks),
                Collections.emptyList(),
                Collections.emptyList(),
                allowCheckpointsAfterTasksFinished);
    }

    private Set<String> getBlockedJobIdsForRescaling(final String snapshotGroup) {
        // after "rescale-<rescaled_job_id>:"
        String targetJobsString = snapshotGroup;
        targetJobsString = targetJobsString.substring(targetJobsString.indexOf(":") + 1);
        String[] jobIdsStr = targetJobsString.split(",");
        return new HashSet<>(Arrays.asList(jobIdsStr));
    }

    private CheckpointPlan calculateWithAllTasksRunning(final String snapshotGroup) {
        List<ExecutionVertex> targetedTasks = new ArrayList<>();
        List<ExecutionVertex> targetedSourceTasks = new ArrayList<>();
        if (snapshotGroup.startsWith("rescale-")) {
            String rescaledTaskId =
                    snapshotGroup.substring("rescale-".length(), snapshotGroup.indexOf("="));
            ExecutionJobVertex rescaledTask = null;
            for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
                if (jobVertex.getJobVertexId().toHexString().equals(rescaledTaskId)) {
                    rescaledTask = jobVertex;
                    break;
                }
            }
            Set<String> triggeredTaskIds = getBlockedJobIdsForRescaling(snapshotGroup);
            List<ExecutionJobVertex> firstLevelDownstreams = new ArrayList<>();
            targetedTasks.addAll(Arrays.asList(rescaledTask.getTaskVertices()));
            for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
                if (triggeredTaskIds.contains(jobVertex.getJobVertexId().toHexString())) {
                    targetedSourceTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                } else {
                    if (jobVertex.getJobVertex().isDownStreamOf(rescaledTask.getJobVertex())) {
                        targetedTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                        //                        firstLevelDownstreams.add(jobVertex);
                    }
                }
            }
            // add second-level downstreams as terminator
            //            for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
            //                for (ExecutionJobVertex first : firstLevelDownstreams) {
            //                    if (jobVertex.getJobVertex().isDownStreamOf(first.getJobVertex()))
            // {
            //
            // targetedTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
            //                        break;
            //                    }
            //                }
            //            }
        } else {
            for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
                if (jobVertex.getJobVertex().isInputVertex()
                        && Objects.equals(jobVertex.getSnapshotGroup(), snapshotGroup)) {
                    targetedSourceTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                }
                if (Objects.equals(jobVertex.getSnapshotGroup(), snapshotGroup)
                        || jobVertex.getJobVertex().isDownStreamOfSnapshotGroup(snapshotGroup)) {
                    targetedTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                } else {
                    if (jobVertex.getJobVertex().isDirectUpstreamOfSnapshotGroup(snapshotGroup)) {
                        targetedSourceTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                    }
                }
            }
        }

        List<Execution> executionsToTrigger =
                targetedSourceTasks.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());

        List<Execution> tasksToWaitFor = createTaskToWaitFor(targetedTasks);

        return new DefaultCheckpointPlan(
                Collections.unmodifiableList(executionsToTrigger),
                Collections.unmodifiableList(tasksToWaitFor),
                Collections.unmodifiableList(targetedTasks),
                Collections.emptyList(),
                Collections.emptyList(),
                allowCheckpointsAfterTasksFinished);
    }

    /**
     * Calculates the checkpoint plan after some tasks have finished. We iterate the job graph to
     * find the task that is still running, but do not has precedent running tasks.
     *
     * @return The plan of this checkpoint.
     */
    private CheckpointPlan calculateAfterTasksFinished() {
        // First collect the task running status into BitSet so that we could
        // do JobVertex level judgement for some vertices and avoid time-consuming
        // access to volatile isFinished flag of Execution.
        Map<JobVertexID, BitSet> taskRunningStatusByVertex = collectTaskRunningStatus();

        List<Execution> tasksToTrigger = new ArrayList<>();
        List<Execution> tasksToWaitFor = new ArrayList<>();
        List<ExecutionVertex> tasksToCommitTo = new ArrayList<>();
        List<Execution> finishedTasks = new ArrayList<>();
        List<ExecutionJobVertex> fullyFinishedJobVertex = new ArrayList<>();

        for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
            BitSet taskRunningStatus = taskRunningStatusByVertex.get(jobVertex.getJobVertexId());

            if (taskRunningStatus.cardinality() == 0) {
                fullyFinishedJobVertex.add(jobVertex);

                for (ExecutionVertex task : jobVertex.getTaskVertices()) {
                    finishedTasks.add(task.getCurrentExecutionAttempt());
                }

                continue;
            }

            List<JobEdge> prevJobEdges = jobVertex.getJobVertex().getInputs();

            // this is an optimization: we determine at the JobVertex level if some tasks can even
            // be eligible for being in the "triggerTo" set.
            boolean someTasksMustBeTriggered =
                    someTasksMustBeTriggered(taskRunningStatusByVertex, prevJobEdges);

            for (int i = 0; i < jobVertex.getTaskVertices().length; ++i) {
                ExecutionVertex task = jobVertex.getTaskVertices()[i];
                if (taskRunningStatus.get(task.getParallelSubtaskIndex())) {
                    tasksToWaitFor.add(task.getCurrentExecutionAttempt());
                    tasksToCommitTo.add(task);

                    if (someTasksMustBeTriggered) {
                        boolean hasRunningPrecedentTasks =
                                hasRunningPrecedentTasks(
                                        task, prevJobEdges, taskRunningStatusByVertex);

                        if (!hasRunningPrecedentTasks) {
                            tasksToTrigger.add(task.getCurrentExecutionAttempt());
                        }
                    }
                } else {
                    finishedTasks.add(task.getCurrentExecutionAttempt());
                }
            }
        }

        return new DefaultCheckpointPlan(
                Collections.unmodifiableList(tasksToTrigger),
                Collections.unmodifiableList(tasksToWaitFor),
                Collections.unmodifiableList(tasksToCommitTo),
                Collections.unmodifiableList(finishedTasks),
                Collections.unmodifiableList(fullyFinishedJobVertex),
                allowCheckpointsAfterTasksFinished);
    }

    private CheckpointPlan calculateAfterTasksFinished(final String snapshotGroup) {
        if (snapshotGroup.startsWith("rescale-")) {
            return calculateAfterTasksFinished();
        }
        // First collect the task running status into BitSet so that we could
        // do JobVertex level judgement for some vertices and avoid time-consuming
        // access to volatile isFinished flag of Execution.
        Map<JobVertexID, BitSet> taskRunningStatusByVertex =
                collectTaskRunningStatus(snapshotGroup);

        List<Execution> tasksToTrigger = new ArrayList<>();
        List<Execution> tasksToWaitFor = new ArrayList<>();
        List<ExecutionVertex> tasksToCommitTo = new ArrayList<>();
        List<Execution> finishedTasks = new ArrayList<>();
        List<ExecutionJobVertex> fullyFinishedJobVertex = new ArrayList<>();

        for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
            if (Objects.equals(jobVertex.getSnapshotGroup(), snapshotGroup)
                    || jobVertex.getJobVertex().isDownStreamOfSnapshotGroup(snapshotGroup)) {
                BitSet taskRunningStatus =
                        taskRunningStatusByVertex.get(jobVertex.getJobVertexId());

                if (taskRunningStatus.cardinality() == 0) {
                    fullyFinishedJobVertex.add(jobVertex);

                    for (ExecutionVertex task : jobVertex.getTaskVertices()) {
                        finishedTasks.add(task.getCurrentExecutionAttempt());
                    }

                    continue;
                }

                List<JobEdge> prevJobEdges = jobVertex.getJobVertex().getInputs();

                // this is an optimization: we determine at the JobVertex level if some tasks
                // can even
                // be eligible for being in the "triggerTo" set.
                boolean someTasksMustBeTriggered =
                        someTasksMustBeTriggered(taskRunningStatusByVertex, prevJobEdges);

                for (int i = 0; i < jobVertex.getTaskVertices().length; ++i) {
                    ExecutionVertex task = jobVertex.getTaskVertices()[i];
                    if (taskRunningStatus.get(task.getParallelSubtaskIndex())) {
                        tasksToWaitFor.add(task.getCurrentExecutionAttempt());
                        tasksToCommitTo.add(task);

                        if (someTasksMustBeTriggered) {
                            boolean hasRunningPrecedentTasks =
                                    hasRunningPrecedentTasks(
                                            task, prevJobEdges, taskRunningStatusByVertex);

                            if (!hasRunningPrecedentTasks) {
                                tasksToTrigger.add(task.getCurrentExecutionAttempt());
                            }
                        }
                    } else {
                        finishedTasks.add(task.getCurrentExecutionAttempt());
                    }
                }
            }
        }

        return new DefaultCheckpointPlan(
                Collections.unmodifiableList(tasksToTrigger),
                Collections.unmodifiableList(tasksToWaitFor),
                Collections.unmodifiableList(tasksToCommitTo),
                Collections.unmodifiableList(finishedTasks),
                Collections.unmodifiableList(fullyFinishedJobVertex),
                allowCheckpointsAfterTasksFinished);
    }

    private boolean someTasksMustBeTriggered(
            Map<JobVertexID, BitSet> runningTasksByVertex, List<JobEdge> prevJobEdges) {

        for (JobEdge jobEdge : prevJobEdges) {
            DistributionPattern distributionPattern = jobEdge.getDistributionPattern();
            BitSet upstreamRunningStatus =
                    runningTasksByVertex.get(jobEdge.getSource().getProducer().getID());

            if (hasActiveUpstreamVertex(distributionPattern, upstreamRunningStatus)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Every task must have active upstream tasks if
     *
     * <ol>
     *   <li>ALL_TO_ALL connection and some predecessors are still running.
     *   <li>POINTWISE connection and all predecessors are still running.
     * </ol>
     *
     * @param distribution The distribution pattern between the upstream vertex and the current
     *     vertex.
     * @param upstreamRunningTasks The running tasks of the upstream vertex.
     * @return Whether every task of the current vertex is connected to some active predecessors.
     */
    private boolean hasActiveUpstreamVertex(
            DistributionPattern distribution, BitSet upstreamRunningTasks) {
        return (distribution == DistributionPattern.ALL_TO_ALL
                        && upstreamRunningTasks.cardinality() > 0)
                || (distribution == DistributionPattern.POINTWISE
                        && upstreamRunningTasks.cardinality() == upstreamRunningTasks.size());
    }

    private boolean hasRunningPrecedentTasks(
            ExecutionVertex vertex,
            List<JobEdge> prevJobEdges,
            Map<JobVertexID, BitSet> taskRunningStatusByVertex) {

        InternalExecutionGraphAccessor executionGraphAccessor = vertex.getExecutionGraphAccessor();

        for (int i = 0; i < prevJobEdges.size(); ++i) {
            if (prevJobEdges.get(i).getDistributionPattern() == DistributionPattern.POINTWISE) {
                for (IntermediateResultPartitionID consumedPartitionId :
                        vertex.getConsumedPartitionGroup(i)) {
                    ExecutionVertex precedentTask =
                            executionGraphAccessor
                                    .getResultPartitionOrThrow(consumedPartitionId)
                                    .getProducer();
                    BitSet precedentVertexRunningStatus =
                            taskRunningStatusByVertex.get(precedentTask.getJobvertexId());

                    if (precedentVertexRunningStatus.get(precedentTask.getParallelSubtaskIndex())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Collects the task running status for each job vertex.
     *
     * @return The task running status for each job vertex.
     */
    @VisibleForTesting
    Map<JobVertexID, BitSet> collectTaskRunningStatus() {
        Map<JobVertexID, BitSet> runningStatusByVertex = new HashMap<>();

        for (ExecutionJobVertex vertex : jobVerticesInTopologyOrder) {
            BitSet runningTasks = new BitSet(vertex.getTaskVertices().length);

            for (int i = 0; i < vertex.getTaskVertices().length; ++i) {
                if (!vertex.getTaskVertices()[i].getCurrentExecutionAttempt().isFinished()) {
                    runningTasks.set(i);
                }
            }

            runningStatusByVertex.put(vertex.getJobVertexId(), runningTasks);
        }

        return runningStatusByVertex;
    }

    @VisibleForTesting
    Map<JobVertexID, BitSet> collectTaskRunningStatus(final String snapshotGroup) {
        Map<JobVertexID, BitSet> runningStatusByVertex = new HashMap<>();

        for (ExecutionJobVertex vertex : jobVerticesInTopologyOrder) {
            if (vertex.getSnapshotGroup() != null) {
                if (vertex.getSnapshotGroup().equals(snapshotGroup)) {
                    BitSet runningTasks = new BitSet(vertex.getTaskVertices().length);

                    for (int i = 0; i < vertex.getTaskVertices().length; ++i) {
                        if (!vertex.getTaskVertices()[i]
                                .getCurrentExecutionAttempt()
                                .isFinished()) {
                            runningTasks.set(i);
                        }
                    }

                    runningStatusByVertex.put(vertex.getJobVertexId(), runningTasks);
                }
            }
        }

        return runningStatusByVertex;
    }

    private List<Execution> createTaskToWaitFor(List<ExecutionVertex> tasks) {
        List<Execution> tasksToAck = new ArrayList<>(tasks.size());
        for (ExecutionVertex task : tasks) {
            tasksToAck.add(task.getCurrentExecutionAttempt());
        }

        return tasksToAck;
    }

    private boolean hasFinishedTask(final String snapshotGroup) {
        for (ExecutionJobVertex vertex : jobVerticesInTopologyOrder) {
            if (vertex.getSnapshotGroup() != null) {
                if (vertex.getSnapshotGroup().equals(snapshotGroup)) {
                    for (int i = 0; i < vertex.getTaskVertices().length; ++i) {
                        if (vertex.getTaskVertices()[i].getCurrentExecutionAttempt().isFinished()) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
