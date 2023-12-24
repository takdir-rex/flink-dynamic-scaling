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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.EdgeManager;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.failover.flip1.SchedulingPipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalEdge;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Adapter of {@link ExecutionGraph} to {@link SchedulingTopology}. */
public class DefaultExecutionTopology implements SchedulingTopology {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionTopology.class);

    private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById;

    private final List<DefaultExecutionVertex> executionVerticesList;

    private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById;

    private final Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion> pipelinedRegionsByVertex;

    private final List<DefaultSchedulingPipelinedRegion> pipelinedRegions;

    private final EdgeManager edgeManager;

    private final DefaultExecutionGraph executionGraph;

    private SchedulingStrategy schedulingStrategy = null;

    private DefaultExecutionTopology(
            Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById,
            List<DefaultExecutionVertex> executionVerticesList,
            Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById,
            Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion> pipelinedRegionsByVertex,
            List<DefaultSchedulingPipelinedRegion> pipelinedRegions,
            EdgeManager edgeManager,
            DefaultExecutionGraph executionGraph) {
        this.executionVerticesById = checkNotNull(executionVerticesById);
        this.executionVerticesList = checkNotNull(executionVerticesList);
        this.resultPartitionsById = checkNotNull(resultPartitionsById);
        this.pipelinedRegionsByVertex = checkNotNull(pipelinedRegionsByVertex);
        this.pipelinedRegions = checkNotNull(pipelinedRegions);
        this.edgeManager = edgeManager;
        this.executionGraph = executionGraph;
    }

    @Override
    public Iterable<DefaultExecutionVertex> getVertices() {
        return Collections.unmodifiableList(executionVerticesList);
    }

    @Override
    public DefaultExecutionVertex getVertex(final ExecutionVertexID executionVertexId) {
        final DefaultExecutionVertex executionVertex = executionVerticesById.get(executionVertexId);
        if (executionVertex == null) {
            throw new IllegalArgumentException("can not find vertex: " + executionVertexId);
        }
        return executionVertex;
    }

    @Override
    public DefaultResultPartition getResultPartition(
            final IntermediateResultPartitionID intermediateResultPartitionId) {
        final DefaultResultPartition resultPartition =
                resultPartitionsById.get(intermediateResultPartitionId);
        if (resultPartition == null) {
            throw new IllegalArgumentException(
                    "can not find partition: " + intermediateResultPartitionId);
        }
        return resultPartition;
    }

    /**
     * Rescaling mechanisms: A --> B (rescaled operator) --> C a. trigger a global checkpoint b.
     * suspend process input in A after the checkpoint barrier sent by A c. when the checkpoint
     * completed, update execution graph and topology at Job Manager (step 1 below) d. update result
     * partitions of A (step 3 below) e. cancel all tasks of B f. re-partitions the states of B to
     * its subtasks and restore their states g. submit tasks B h. update input channels of C (step 5
     * below) i. resume process input in A (unblock channel)
     *
     * @param rescaledJobIdHexString
     * @param newParallelism
     * @return
     */
    @Override
    public void changeParallelism(
            String rescaledJobIdHexString, int newParallelism, String[] blockedJobIdsHexString) {
        final ExecutionJobVertex rescaledEjv =
                executionGraph.getJobVertex(JobVertexID.fromHexString(rescaledJobIdHexString));
        // 1. update execution graph and topology at Job Manager
        List<ExecutionVertex> newVertices =
                executionGraph.changeParallelism(rescaledEjv, newParallelism);

        // add new vertices as new members of region
        List<ExecutionVertexID> newVerticesID =
                newVertices.stream().map(ExecutionVertex::getID).collect(Collectors.toList());
        List<SchedulingExecutionVertex> newSchedulingExecutionVertices =
                new ArrayList<>(newVertices.size());
        for (ExecutionVertexID id : newVerticesID) {
            newSchedulingExecutionVertices.add(executionVerticesById.get(id));
        }
        PipelinedRegionSchedulingStrategy pipelinedRegionSchedulingStrategy =
                (PipelinedRegionSchedulingStrategy) schedulingStrategy;
        for (SchedulingExecutionVertex vertex : newSchedulingExecutionVertices) {
            final SchedulingPipelinedRegion region =
                    this.getPipelinedRegionOfVertex(vertex.getId());
            pipelinedRegionSchedulingStrategy
                    .getRegionVerticesSorted()
                    .get(region)
                    .add(vertex.getId());
        }

        List<CompletableFuture> subpartitionFutures = new ArrayList<>();
        // 3. also update result partitions of their upstreams
        for (IntermediateResult ir : rescaledEjv.getInputs()) {
            for (ExecutionVertex vertex : ir.getProducer().getTaskVertices()) {
                subpartitionFutures.add(vertex.updateSubpartitionParallelism());
            }
        }

        final DefaultScheduler scheduler =
                (DefaultScheduler) pipelinedRegionSchedulingStrategy.getSchedulerOperations();

        // request slot for newly created instances
        scheduler.requestNewSlots(newSchedulingExecutionVertices);

        // futures for waiting restarted tasks to be running
        List<CompletableFuture> runningFutures = new ArrayList<>();
        // schedule restart for rescaled tasks
        Set<ExecutionVertexID> executionVertexIDS = new HashSet<>();
        for (ExecutionVertex vertex : rescaledEjv.getTaskVertices()) {
            executionVertexIDS.add(vertex.getID());
            vertex.startListenRunningFuture();
            runningFutures.add(vertex.getRunningFuture());
        }

        // also schedule restart for direct downstream tasks
        Set<JobVertexID> downstreams =
                scheduleDownstreamRestart(rescaledEjv, executionVertexIDS, runningFutures);

        // restart tasks
        CompletableFuture.allOf(subpartitionFutures.toArray(new CompletableFuture[0]))
                .thenRun(
                        () -> {
                            scheduler.restartTasksForRescaling(executionVertexIDS);
                        });

        CompletableFuture.allOf(runningFutures.toArray(new CompletableFuture[0]))
                .thenRun(
                        () -> {
                            List<CompletableFuture> updateChannelFutures = new ArrayList<>();
                            Set<JobVertex> sencondDownstreams = new HashSet<>();
                            // update input channels of their second downstreams
                            for (JobVertexID jobVertexID : downstreams) {
                                for (IntermediateDataSet producedDataSet :
                                        executionGraph
                                                .getJobVertex(jobVertexID)
                                                .getJobVertex()
                                                .getProducedDataSets()) {
                                    for (JobEdge outputEdge : producedDataSet.getConsumers()) {
                                        ExecutionJobVertex downstreamEjv =
                                                executionGraph.getJobVertex(
                                                        outputEdge.getTarget().getID());
                                        if (sencondDownstreams.add(
                                                downstreamEjv
                                                        .getJobVertex())) { // ensure unique vertex
                                            // executed once
                                            // updateInputChannels
                                            for (ExecutionVertex vtx :
                                                    downstreamEjv.getTaskVertices()) {
                                                Execution exVtx = vtx.getCurrentExecutionAttempt();
                                                try {
                                                    final List<InputGateDeploymentDescriptor>
                                                            inputGateDeploymentDescriptors =
                                                                    TaskDeploymentDescriptorFactory
                                                                            .fromExecutionVertex(
                                                                                    vtx,
                                                                                    exVtx
                                                                                            .getAttemptNumber())
                                                                            .createInputGateDeploymentDescriptors(
                                                                                    producedDataSet
                                                                                            .getId());
                                                    updateChannelFutures.add(
                                                            exVtx.updateInputChannels(
                                                                    inputGateDeploymentDescriptors));
                                                } catch (IOException e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        });
    }

    private Set<JobVertexID> scheduleDownstreamRestart(
            ExecutionJobVertex ejv,
            Set<ExecutionVertexID> executionVertexIDS,
            List<CompletableFuture> runningFutures) {
        Set<JobVertexID> downstreams = new HashSet<>();
        for (IntermediateDataSet producedDataSet : ejv.getJobVertex().getProducedDataSets()) {
            for (JobEdge outputEdge : producedDataSet.getConsumers()) {
                ExecutionJobVertex downstreamEjv =
                        executionGraph.getJobVertex(outputEdge.getTarget().getID());
                if (downstreams.add(downstreamEjv.getJobVertexId())) {
                    for (ExecutionVertex vtx : downstreamEjv.getTaskVertices()) {
                        executionVertexIDS.add(vtx.getID());
                        vtx.startListenRunningFuture();
                        runningFutures.add(vtx.getRunningFuture());
                    }
                }
            }
        }
        return downstreams;
    }

    @Override
    public Iterable<DefaultSchedulingPipelinedRegion> getAllPipelinedRegions() {
        checkNotNull(pipelinedRegions);

        return Collections.unmodifiableCollection(pipelinedRegions);
    }

    @Override
    public DefaultSchedulingPipelinedRegion getPipelinedRegionOfVertex(
            final ExecutionVertexID vertexId) {
        checkNotNull(pipelinedRegionsByVertex);

        final DefaultSchedulingPipelinedRegion pipelinedRegion =
                pipelinedRegionsByVertex.get(vertexId);
        if (pipelinedRegion == null) {
            throw new IllegalArgumentException("Unknown execution vertex " + vertexId);
        }
        return pipelinedRegion;
    }

    public EdgeManager getEdgeManager() {
        return edgeManager;
    }

    public static DefaultExecutionTopology fromExecutionGraph(
            DefaultExecutionGraph executionGraph) {
        checkNotNull(executionGraph, "execution graph can not be null");

        EdgeManager edgeManager = executionGraph.getEdgeManager();

        List<JobVertex> topologicallySortedJobVertices =
                IterableUtils.toStream(executionGraph.getVerticesTopologically())
                        .map(ExecutionJobVertex::getJobVertex)
                        .collect(Collectors.toList());

        Iterable<DefaultLogicalPipelinedRegion> logicalPipelinedRegions =
                DefaultLogicalTopology.fromTopologicallySortedJobVertices(
                                topologicallySortedJobVertices)
                        .getAllPipelinedRegions();

        ExecutionGraphIndex executionGraphIndex =
                computeExecutionGraphIndex(
                        executionGraph.getAllExecutionVertices(),
                        executionGraph.getTotalNumberOfVertices(),
                        logicalPipelinedRegions,
                        edgeManager);

        IndexedPipelinedRegions indexedPipelinedRegions =
                computePipelinedRegions(
                        logicalPipelinedRegions,
                        executionGraphIndex.sortedExecutionVerticesInPipelinedRegion::get,
                        executionGraphIndex.executionVerticesById::get,
                        executionGraphIndex.resultPartitionsById::get);

        ensureCoLocatedVerticesInSameRegion(
                indexedPipelinedRegions.pipelinedRegions, executionGraph);

        return new DefaultExecutionTopology(
                executionGraphIndex.executionVerticesById,
                executionGraphIndex.executionVerticesList,
                executionGraphIndex.resultPartitionsById,
                indexedPipelinedRegions.pipelinedRegionsByVertex,
                indexedPipelinedRegions.pipelinedRegions,
                edgeManager,
                executionGraph);
    }

    public List<SchedulingExecutionVertex> addExecutionVertices(
            ExecutionVertexID sibling, List<ExecutionVertex> newExecutionVertices) {
        Map<ExecutionVertexID, DefaultExecutionVertex> newVertices = new HashMap<>();
        DefaultSchedulingPipelinedRegion pipelinedRegion = this.getPipelinedRegionOfVertex(sibling);
        // computeExecutionGraphIndex and computePipelinedRegions
        for (ExecutionVertex vertex : newExecutionVertices) {
            List<DefaultResultPartition> producedPartitions =
                    generateProducedSchedulingResultPartition(
                            vertex.getProducedPartitions(),
                            edgeManager::getConsumerVertexGroupsForPartition,
                            executionVerticesById::get);
            producedPartitions.forEach(
                    partition -> resultPartitionsById.put(partition.getId(), partition));

            DefaultExecutionVertex schedulingVertex =
                    generateSchedulingExecutionVertex(
                            vertex,
                            producedPartitions,
                            edgeManager.getConsumedPartitionGroupsForVertex(vertex.getID()),
                            resultPartitionsById::get);
            executionVerticesById.put(schedulingVertex.getId(), schedulingVertex);
            executionVerticesList.add(schedulingVertex);
            pipelinedRegionsByVertex.put(
                    schedulingVertex.getId(), pipelinedRegion); // in computePipelinedRegions
            newVertices.put(vertex.getID(), schedulingVertex);
        }
        pipelinedRegion.addVertices(newVertices); // in computePipelinedRegions
        return newVertices.values().stream().collect(Collectors.toList());
    }

    private static ExecutionGraphIndex computeExecutionGraphIndex(
            Iterable<ExecutionVertex> executionVertices,
            int vertexNumber,
            Iterable<DefaultLogicalPipelinedRegion> logicalPipelinedRegions,
            EdgeManager edgeManager) {
        Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById = new HashMap<>();
        List<DefaultExecutionVertex> executionVerticesList = new ArrayList<>(vertexNumber);
        Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById =
                new HashMap<>();
        Map<DefaultLogicalPipelinedRegion, List<DefaultExecutionVertex>>
                sortedExecutionVerticesInPipelinedRegion = new IdentityHashMap<>();

        Map<JobVertexID, DefaultLogicalPipelinedRegion> logicalPipelinedRegionByJobVertexId =
                new HashMap<>();
        for (DefaultLogicalPipelinedRegion logicalPipelinedRegion : logicalPipelinedRegions) {
            for (LogicalVertex vertex : logicalPipelinedRegion.getVertices()) {
                logicalPipelinedRegionByJobVertexId.put(vertex.getId(), logicalPipelinedRegion);
            }
        }

        for (ExecutionVertex vertex : executionVertices) {
            List<DefaultResultPartition> producedPartitions =
                    generateProducedSchedulingResultPartition(
                            vertex.getProducedPartitions(),
                            edgeManager::getConsumerVertexGroupsForPartition,
                            executionVerticesById::get);

            producedPartitions.forEach(
                    partition -> resultPartitionsById.put(partition.getId(), partition));

            DefaultExecutionVertex schedulingVertex =
                    generateSchedulingExecutionVertex(
                            vertex,
                            producedPartitions,
                            edgeManager.getConsumedPartitionGroupsForVertex(vertex.getID()),
                            resultPartitionsById::get);
            executionVerticesById.put(schedulingVertex.getId(), schedulingVertex);
            sortedExecutionVerticesInPipelinedRegion
                    .computeIfAbsent(
                            logicalPipelinedRegionByJobVertexId.get(
                                    schedulingVertex.getId().getJobVertexId()),
                            ignore -> new ArrayList<>())
                    .add(schedulingVertex);
            executionVerticesList.add(schedulingVertex);
        }
        return new ExecutionGraphIndex(
                executionVerticesById,
                executionVerticesList,
                sortedExecutionVerticesInPipelinedRegion,
                resultPartitionsById);
    }

    private static List<DefaultResultPartition> generateProducedSchedulingResultPartition(
            Map<IntermediateResultPartitionID, IntermediateResultPartition>
                    producedIntermediatePartitions,
            Function<IntermediateResultPartitionID, List<ConsumerVertexGroup>>
                    partitionConsumerVertexGroups,
            Function<ExecutionVertexID, DefaultExecutionVertex> executionVertexRetriever) {

        List<DefaultResultPartition> producedSchedulingPartitions =
                new ArrayList<>(producedIntermediatePartitions.size());

        producedIntermediatePartitions
                .values()
                .forEach(
                        irp ->
                                producedSchedulingPartitions.add(
                                        new DefaultResultPartition(
                                                irp.getPartitionId(),
                                                irp.getIntermediateResult().getId(),
                                                irp.getResultType(),
                                                () ->
                                                        irp.isConsumable()
                                                                ? ResultPartitionState.CONSUMABLE
                                                                : ResultPartitionState.CREATED,
                                                partitionConsumerVertexGroups.apply(
                                                        irp.getPartitionId()),
                                                executionVertexRetriever,
                                                irp::getConsumedPartitionGroups)));

        return producedSchedulingPartitions;
    }

    private static DefaultExecutionVertex generateSchedulingExecutionVertex(
            ExecutionVertex vertex,
            List<DefaultResultPartition> producedPartitions,
            List<ConsumedPartitionGroup> consumedPartitionGroups,
            Function<IntermediateResultPartitionID, DefaultResultPartition>
                    resultPartitionRetriever) {

        DefaultExecutionVertex schedulingVertex =
                new DefaultExecutionVertex(
                        vertex.getID(),
                        producedPartitions,
                        vertex::getExecutionState,
                        consumedPartitionGroups,
                        resultPartitionRetriever,
                        vertex.getJobVertex().getSnapshotGroup());

        producedPartitions.forEach(partition -> partition.setProducer(schedulingVertex));

        return schedulingVertex;
    }

    private static IndexedPipelinedRegions computePipelinedRegions(
            Iterable<DefaultLogicalPipelinedRegion> logicalPipelinedRegions,
            Function<DefaultLogicalPipelinedRegion, List<DefaultExecutionVertex>>
                    sortedExecutionVerticesInPipelinedRegion,
            Function<ExecutionVertexID, DefaultExecutionVertex> executionVertexRetriever,
            Function<IntermediateResultPartitionID, DefaultResultPartition>
                    resultPartitionRetriever) {

        long buildRegionsStartTime = System.nanoTime();

        Set<Set<SchedulingExecutionVertex>> rawPipelinedRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // A SchedulingPipelinedRegion can be derived from just one LogicalPipelinedRegion.
        // Thus, we can traverse all LogicalPipelinedRegions and convert them into
        // SchedulingPipelinedRegions one by one. The LogicalPipelinedRegions and
        // SchedulingPipelinedRegions are both connected with inter-region blocking edges.
        for (DefaultLogicalPipelinedRegion logicalPipelinedRegion : logicalPipelinedRegions) {

            List<DefaultExecutionVertex> schedulingExecutionVertices =
                    sortedExecutionVerticesInPipelinedRegion.apply(logicalPipelinedRegion);

            if (containsIntraRegionAllToAllEdge(logicalPipelinedRegion)) {
                // For edges inside one LogicalPipelinedRegion, if there is any all-to-all edge, it
                // could be under two circumstances:
                //
                // 1. Pipelined all-to-all edge:
                //     Pipelined all-to-all edge will connect all vertices pipelined. Therefore,
                // all execution vertices derived from this LogicalPipelinedRegion should be in one
                // SchedulingPipelinedRegion.
                //
                // 2. Blocking all-to-all edge:
                //     For intra-region blocking all-to-all edge, we must make sure all the vertices
                // are inside one SchedulingPipelinedRegion, so that there will be no deadlock
                // happens during scheduling. For more details about this case, please refer to
                // FLINK-17330 (https://issues.apache.org/jira/browse/FLINK-17330).
                //
                // Therefore, if a LogicalPipelinedRegion contains any intra-region all-to-all
                // edge, we just convert the entire LogicalPipelinedRegion to a sole
                // SchedulingPipelinedRegion directly.
                rawPipelinedRegions.add(new HashSet<>(schedulingExecutionVertices));
            } else {
                // If there are only pointwise edges inside the LogicalPipelinedRegion, we can use
                // SchedulingPipelinedRegionComputeUtil to compute the regions with O(N) computation
                // complexity.
                rawPipelinedRegions.addAll(
                        SchedulingPipelinedRegionComputeUtil.computePipelinedRegions(
                                schedulingExecutionVertices,
                                executionVertexRetriever,
                                resultPartitionRetriever));
            }
        }

        Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion> pipelinedRegionsByVertex =
                new HashMap<>();
        List<DefaultSchedulingPipelinedRegion> pipelinedRegions = new ArrayList<>();

        for (Set<? extends SchedulingExecutionVertex> rawPipelinedRegion : rawPipelinedRegions) {
            //noinspection unchecked
            final DefaultSchedulingPipelinedRegion pipelinedRegion =
                    new DefaultSchedulingPipelinedRegion(
                            (Set<DefaultExecutionVertex>) rawPipelinedRegion,
                            resultPartitionRetriever);
            pipelinedRegions.add(pipelinedRegion);

            for (SchedulingExecutionVertex executionVertex : rawPipelinedRegion) {
                pipelinedRegionsByVertex.put(executionVertex.getId(), pipelinedRegion);
            }
        }

        long buildRegionsDuration = (System.nanoTime() - buildRegionsStartTime) / 1_000_000;
        LOG.info(
                "Built {} pipelined regions in {} ms",
                pipelinedRegions.size(),
                buildRegionsDuration);

        return new IndexedPipelinedRegions(pipelinedRegionsByVertex, pipelinedRegions);
    }

    /**
     * Check if the {@link DefaultLogicalPipelinedRegion} contains intra-region all-to-all edges or
     * not.
     */
    private static boolean containsIntraRegionAllToAllEdge(
            DefaultLogicalPipelinedRegion logicalPipelinedRegion) {
        for (LogicalVertex vertex : logicalPipelinedRegion.getVertices()) {
            for (LogicalEdge inputEdge : vertex.getInputs()) {
                if (inputEdge.getDistributionPattern() == DistributionPattern.ALL_TO_ALL
                        && logicalPipelinedRegion.contains(inputEdge.getProducerVertexId())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Co-location constraints are only used for iteration head and tail. A paired head and tail
     * needs to be in the same pipelined region so that they can be restarted together.
     */
    private static void ensureCoLocatedVerticesInSameRegion(
            List<DefaultSchedulingPipelinedRegion> pipelinedRegions,
            ExecutionGraph executionGraph) {

        final Map<CoLocationConstraint, DefaultSchedulingPipelinedRegion> constraintToRegion =
                new HashMap<>();
        for (DefaultSchedulingPipelinedRegion region : pipelinedRegions) {
            for (DefaultExecutionVertex vertex : region.getVertices()) {
                final CoLocationConstraint constraint =
                        getCoLocationConstraint(vertex.getId(), executionGraph);
                if (constraint != null) {
                    final DefaultSchedulingPipelinedRegion regionOfConstraint =
                            constraintToRegion.get(constraint);
                    checkState(
                            regionOfConstraint == null || regionOfConstraint == region,
                            "co-located tasks must be in the same pipelined region");
                    constraintToRegion.putIfAbsent(constraint, region);
                }
            }
        }
    }

    private static CoLocationConstraint getCoLocationConstraint(
            ExecutionVertexID executionVertexId, ExecutionGraph executionGraph) {

        CoLocationGroup coLocationGroup =
                Objects.requireNonNull(
                                executionGraph.getJobVertex(executionVertexId.getJobVertexId()))
                        .getCoLocationGroup();
        return coLocationGroup == null
                ? null
                : coLocationGroup.getLocationConstraint(executionVertexId.getSubtaskIndex());
    }

    public void setSchedulingStrategy(SchedulingStrategy schedulingStrategy) {
        this.schedulingStrategy = schedulingStrategy;
    }

    private static class ExecutionGraphIndex {
        private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById;
        private final List<DefaultExecutionVertex> executionVerticesList;
        private final Map<DefaultLogicalPipelinedRegion, List<DefaultExecutionVertex>>
                sortedExecutionVerticesInPipelinedRegion;
        private final Map<IntermediateResultPartitionID, DefaultResultPartition>
                resultPartitionsById;

        private ExecutionGraphIndex(
                Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById,
                List<DefaultExecutionVertex> executionVerticesList,
                Map<DefaultLogicalPipelinedRegion, List<DefaultExecutionVertex>>
                        sortedExecutionVerticesInPipelinedRegion,
                Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById) {
            this.executionVerticesById = checkNotNull(executionVerticesById);
            this.executionVerticesList = checkNotNull(executionVerticesList);
            this.sortedExecutionVerticesInPipelinedRegion =
                    checkNotNull(sortedExecutionVerticesInPipelinedRegion);
            this.resultPartitionsById = checkNotNull(resultPartitionsById);
        }
    }

    private static class IndexedPipelinedRegions {
        private final Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion>
                pipelinedRegionsByVertex;
        private final List<DefaultSchedulingPipelinedRegion> pipelinedRegions;

        private IndexedPipelinedRegions(
                Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion> pipelinedRegionsByVertex,
                List<DefaultSchedulingPipelinedRegion> pipelinedRegions) {
            this.pipelinedRegionsByVertex = checkNotNull(pipelinedRegionsByVertex);
            this.pipelinedRegions = checkNotNull(pipelinedRegions);
        }
    }
}
