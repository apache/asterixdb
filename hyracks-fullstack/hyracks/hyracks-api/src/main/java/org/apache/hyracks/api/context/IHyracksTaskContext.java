/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.context;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.IOperatorEnvironment;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;
import org.apache.hyracks.api.resources.IDeallocatableRegistry;
import org.apache.hyracks.api.result.IResultPartitionManager;
import org.apache.hyracks.util.IThreadStats;
import org.apache.hyracks.util.IThreadStatsCollector;

public interface IHyracksTaskContext
        extends IHyracksCommonContext, IWorkspaceFileFactory, IDeallocatableRegistry, IOperatorEnvironment {
    IHyracksJobletContext getJobletContext();

    TaskAttemptId getTaskAttemptId();

    ICounterContext getCounterContext();

    ExecutorService getExecutorService();

    IResultPartitionManager getResultPartitionManager();

    void sendApplicationMessageToCC(Serializable message, DeploymentId deploymentId) throws Exception;

    void sendApplicationMessageToCC(byte[] message, DeploymentId deploymentId) throws Exception;

    void setSharedObject(Object object);

    Object getSharedObject();

    byte[] getJobParameter(byte[] name, int start, int length) throws HyracksException;

    Set<JobFlag> getJobFlags();

    IStatsCollector getStatsCollector();

    IWarningCollector getWarningCollector();

    /**
     * Subscribes the caller thread to {@code threadStatsCollector}
     *
     * @param threadStatsCollector
     */
    void subscribeThreadToStats(IThreadStatsCollector threadStatsCollector);

    /**
     * Unsubscribes the caller thread from all thread stats collectors known to this task
     */
    void unsubscribeThreadFromStats();

    /**
     * Gets the caller thread's stats
     *
     * @return the thread stats
     */
    IThreadStats getThreadStats();
}
