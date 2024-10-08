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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Container class for operator/task specific information which are stored at the {@link
 * ExecutionJobVertex}. This information is shared by all sub tasks of this operator.
 */
public class TaskInformation implements Serializable {

    private static final long serialVersionUID = -9006218793155953789L;

    /** Job vertex id of the associated job vertex. */
    private final JobVertexID jobVertexId;

    private final JobVertex jobVertex;

    /** Name of the task. */
    private final String taskName;

    private int numberOfSubtasks;

    /** The maximum parallelism == number of key groups. */
    private final int maxNumberOfSubtasks;

    /** Class name of the invokable to run. */
    private final String invokableClassName;

    /** Configuration for the task. */
    private final Configuration taskConfiguration;

    public TaskInformation(
            JobVertex jobVertex,
            JobVertexID jobVertexId,
            String taskName,
            int numberOfSubtasks,
            int maxNumberOfSubtasks,
            String invokableClassName,
            Configuration taskConfiguration) {
        this.jobVertex = jobVertex;
        this.jobVertexId = Preconditions.checkNotNull(jobVertexId);
        this.taskName = Preconditions.checkNotNull(taskName);
        this.numberOfSubtasks = numberOfSubtasks;
        this.maxNumberOfSubtasks = maxNumberOfSubtasks;
        this.invokableClassName = Preconditions.checkNotNull(invokableClassName);
        this.taskConfiguration = Preconditions.checkNotNull(taskConfiguration);
    }

    public TaskInformation(
            JobVertexID jobVertexId,
            String taskName,
            int numberOfSubtasks,
            int maxNumberOfSubtasks,
            String invokableClassName,
            Configuration taskConfiguration) {
        this(
                new JobVertex(taskName, jobVertexId),
                jobVertexId,
                taskName,
                numberOfSubtasks,
                maxNumberOfSubtasks,
                invokableClassName,
                taskConfiguration);
    }

    public JobVertexID getJobVertexId() {
        return jobVertexId;
    }

    public String getTaskName() {
        return taskName;
    }

    /** The number of subtasks for this operator. */
    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public int getMaxNumberOfSubtasks() {
        return maxNumberOfSubtasks;
    }

    public String getInvokableClassName() {
        return invokableClassName;
    }

    public Configuration getTaskConfiguration() {
        return taskConfiguration;
    }

    public JobVertex getJobVertex() {
        return jobVertex;
    }
}
