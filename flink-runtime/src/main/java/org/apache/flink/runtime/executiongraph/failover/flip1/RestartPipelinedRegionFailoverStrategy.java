/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that proposes to restart involved regions when a vertex fails. A region is
 * defined by this strategy as tasks that communicate via pipelined data exchange.
 */
public class RestartPipelinedRegionFailoverStrategy implements FailoverStrategy {

    /** The log object used for debugging. */
    private static final Logger LOG =
            LoggerFactory.getLogger(RestartPipelinedRegionFailoverStrategy.class);

    /** The topology containing info about all the vertices and result partitions. */
    private final SchedulingTopology topology;

    /** The checker helps to query result partition availability. */
    private final RegionFailoverResultPartitionAvailabilityChecker
            resultPartitionAvailabilityChecker;

    /**
     * Creates a new failover strategy to restart pipelined regions that works on the given
     * topology. The result partitions are always considered to be available if no data consumption
     * error happens.
     *
     * @param topology containing info about all the vertices and result partitions
     */
    @VisibleForTesting
    public RestartPipelinedRegionFailoverStrategy(SchedulingTopology topology) {
        this(topology, resultPartitionID -> true);
    }

    /**
     * Creates a new failover strategy to restart pipelined regions that works on the given
     * topology.
     *
     * @param topology containing info about all the vertices and result partitions
     * @param resultPartitionAvailabilityChecker helps to query result partition availability
     */
    public RestartPipelinedRegionFailoverStrategy(
            SchedulingTopology topology,
            ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

        this.topology = checkNotNull(topology);
        this.resultPartitionAvailabilityChecker =
                new RegionFailoverResultPartitionAvailabilityChecker(
                        resultPartitionAvailabilityChecker);
    }

    // ------------------------------------------------------------------------
    //  task failure handling
    // ------------------------------------------------------------------------

    /**
     * Returns a set of IDs corresponding to the set of vertices that should be restarted. In this
     * strategy, all task vertices in 'involved' regions are proposed to be restarted. The
     * 'involved' regions are calculated with rules below: 1. The region containing the failed task
     * is always involved 2. If an input result partition of an involved region is not available,
     * i.e. Missing or Corrupted, the region containing the partition producer task is involved 3.
     * If a region is involved, all of its consumer regions are involved
     *
     * @param executionVertexId ID of the failed task
     * @param cause cause of the failure
     * @return set of IDs of vertices to restart
     */
    @Override
    public Set<ExecutionVertexID> getTasksNeedingRestart(
            ExecutionVertexID executionVertexId, Throwable cause) {
        LOG.info("Calculating tasks to restart to recover the failed task {}.", executionVertexId);

        //        topology.changeParallelism(executionVertexId.getJobVertexId(), 2);

        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final SchedulingPipelinedRegion failedRegion =
                topology.getPipelinedRegionOfVertex(executionVertexId);
        if (failedRegion == null) {
            // TODO: show the task name in the log
            throw new IllegalStateException(
                    "Can not find the failover region for task " + executionVertexId, cause);
        }

        // if the failure cause is data consumption error, mark the corresponding data partition to
        // be failed,
        // so that the failover process will try to recover it
        Optional<PartitionException> dataConsumptionException =
                ExceptionUtils.findThrowable(cause, PartitionException.class);
        if (dataConsumptionException.isPresent()) {
            resultPartitionAvailabilityChecker.markResultPartitionFailed(
                    dataConsumptionException.get().getPartitionId().getPartitionId());
        }

        // calculate the tasks to restart based on the result of regions to restart
        Set<ExecutionVertexID> tasksToRestart = new HashSet<>();
        for (SchedulingPipelinedRegion region : getRegionsToRestart(failedRegion)) {
            for (SchedulingExecutionVertex vertex : region.getVertices()) {
                // we do not need to restart tasks which are already in the initial state
                if (vertex.getState() != ExecutionState.CREATED) {
                    tasksToRestart.add(vertex.getId());
                }
            }
        }

        // the previous failed partition will be recovered. remove its failed state from the checker
        if (dataConsumptionException.isPresent()) {
            resultPartitionAvailabilityChecker.removeResultPartitionFromFailedState(
                    dataConsumptionException.get().getPartitionId().getPartitionId());
        }

        LOG.info(
                "{} tasks should be restarted to recover the failed task {}. ",
                tasksToRestart.size(),
                executionVertexId);
        return tasksToRestart;
    }

    /**
     * All 'involved' regions are proposed to be restarted. The 'involved' regions are calculated
     * with rules below: 1. The region containing the failed task is always involved 2. If an input
     * result partition of an involved region is not available, i.e. Missing or Corrupted, the
     * region containing the partition producer task is involved 3. If a region is involved, all of
     * its consumer regions are involved
     */
    private Set<SchedulingPipelinedRegion> getRegionsToRestart(
            SchedulingPipelinedRegion failedRegion) {
        Set<SchedulingPipelinedRegion> regionsToRestart =
                Collections.newSetFromMap(new IdentityHashMap<>());
        Set<SchedulingPipelinedRegion> visitedRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());

        Set<ConsumedPartitionGroup> visitedConsumedResultGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());
        Set<ConsumerVertexGroup> visitedConsumerVertexGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // start from the failed region to visit all involved regions
        Queue<SchedulingPipelinedRegion> regionsToVisit = new ArrayDeque<>();
        visitedRegions.add(failedRegion);
        regionsToVisit.add(failedRegion);
        while (!regionsToVisit.isEmpty()) {
            SchedulingPipelinedRegion regionToRestart = regionsToVisit.poll();

            // an involved region should be restarted
            regionsToRestart.add(regionToRestart);

            // if a needed input result partition is not available, its producer region is involved
            for (IntermediateResultPartitionID consumedPartitionId :
                    getConsumedPartitionsToVisit(regionToRestart, visitedConsumedResultGroups)) {
                if (!resultPartitionAvailabilityChecker.isAvailable(consumedPartitionId)) {
                    SchedulingResultPartition consumedPartition =
                            topology.getResultPartition(consumedPartitionId);
                    SchedulingPipelinedRegion producerRegion =
                            topology.getPipelinedRegionOfVertex(
                                    consumedPartition.getProducer().getId());
                    if (!visitedRegions.contains(producerRegion)) {
                        visitedRegions.add(producerRegion);
                        regionsToVisit.add(producerRegion);
                    }
                }
            }

            // all consumer regions of an involved region should be involved
            for (ExecutionVertexID consumerVertexId :
                    getConsumerVerticesToVisit(regionToRestart, visitedConsumerVertexGroups)) {
                SchedulingPipelinedRegion consumerRegion =
                        topology.getPipelinedRegionOfVertex(consumerVertexId);
                if (!visitedRegions.contains(consumerRegion)) {
                    visitedRegions.add(consumerRegion);
                    regionsToVisit.add(consumerRegion);
                }
            }
        }

        return regionsToRestart;
    }

    private Iterable<IntermediateResultPartitionID> getConsumedPartitionsToVisit(
            SchedulingPipelinedRegion regionToRestart,
            Set<ConsumedPartitionGroup> visitedConsumedResultGroups) {

        final List<ConsumedPartitionGroup> consumedPartitionGroupsToVisit = new ArrayList<>();

        for (SchedulingExecutionVertex vertex : regionToRestart.getVertices()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    vertex.getConsumedPartitionGroups()) {
                if (!visitedConsumedResultGroups.contains(consumedPartitionGroup)) {
                    visitedConsumedResultGroups.add(consumedPartitionGroup);
                    consumedPartitionGroupsToVisit.add(consumedPartitionGroup);
                }
            }
        }

        return IterableUtils.flatMap(consumedPartitionGroupsToVisit, Function.identity());
    }

    private Iterable<ExecutionVertexID> getConsumerVerticesToVisit(
            SchedulingPipelinedRegion regionToRestart,
            Set<ConsumerVertexGroup> visitedConsumerVertexGroups) {
        final List<ConsumerVertexGroup> consumerVertexGroupsToVisit = new ArrayList<>();

        for (SchedulingExecutionVertex vertex : regionToRestart.getVertices()) {
            for (SchedulingResultPartition producedPartition : vertex.getProducedResults()) {
                for (ConsumerVertexGroup consumerVertexGroup :
                        producedPartition.getConsumerVertexGroups()) {
                    if (!visitedConsumerVertexGroups.contains(consumerVertexGroup)) {
                        visitedConsumerVertexGroups.add(consumerVertexGroup);
                        consumerVertexGroupsToVisit.add(consumerVertexGroup);
                    }
                }
            }
        }

        return IterableUtils.flatMap(consumerVertexGroupsToVisit, Function.identity());
    }

    // ------------------------------------------------------------------------
    //  testing
    // ------------------------------------------------------------------------

    /**
     * Returns the failover region that contains the given execution vertex.
     *
     * @return the failover region that contains the given execution vertex
     */
    @VisibleForTesting
    public SchedulingPipelinedRegion getFailoverRegion(ExecutionVertexID vertexID) {
        return topology.getPipelinedRegionOfVertex(vertexID);
    }

    /**
     * A stateful {@link ResultPartitionAvailabilityChecker} which maintains the failed partitions
     * which are not available.
     */
    private static class RegionFailoverResultPartitionAvailabilityChecker
            implements ResultPartitionAvailabilityChecker {

        /** Result partition state checker from the shuffle master. */
        private final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

        /** Records partitions which has caused {@link PartitionException}. */
        private final HashSet<IntermediateResultPartitionID> failedPartitions;

        RegionFailoverResultPartitionAvailabilityChecker(
                ResultPartitionAvailabilityChecker checker) {
            this.resultPartitionAvailabilityChecker = checkNotNull(checker);
            this.failedPartitions = new HashSet<>();
        }

        @Override
        public boolean isAvailable(IntermediateResultPartitionID resultPartitionID) {
            return !failedPartitions.contains(resultPartitionID)
                    && resultPartitionAvailabilityChecker.isAvailable(resultPartitionID);
        }

        public void markResultPartitionFailed(IntermediateResultPartitionID resultPartitionID) {
            failedPartitions.add(resultPartitionID);
        }

        public void removeResultPartitionFromFailedState(
                IntermediateResultPartitionID resultPartitionID) {
            failedPartitions.remove(resultPartitionID);
        }
    }

    /** The factory to instantiate {@link RestartPipelinedRegionFailoverStrategy}. */
    public static class Factory implements FailoverStrategy.Factory {

        @Override
        public FailoverStrategy create(
                final SchedulingTopology topology,
                final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

            return new RestartPipelinedRegionFailoverStrategy(
                    topology, resultPartitionAvailabilityChecker);
        }
    }
}
