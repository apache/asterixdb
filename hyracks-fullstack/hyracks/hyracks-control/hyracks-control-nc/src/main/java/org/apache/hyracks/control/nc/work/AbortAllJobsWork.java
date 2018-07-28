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
package org.apache.hyracks.control.nc.work;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.result.IResultPartitionManager;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AbortAllJobsWork extends SynchronizableWork {

    private static final Logger LOGGER = LogManager.getLogger();
    private final NodeControllerService ncs;
    private final CcId ccId;

    public AbortAllJobsWork(NodeControllerService ncs, CcId ccId) {
        this.ncs = ncs;
        this.ccId = ccId;
    }

    @Override
    protected void doRun() throws Exception {
        LOGGER.info("Aborting all tasks for controller {}", ccId);
        IResultPartitionManager resultPartitionManager = ncs.getResultPartitionManager();
        if (resultPartitionManager == null) {
            LOGGER.log(Level.WARN, "ResultPartitionManager is null on " + ncs.getId());
        }
        Deque<Task> abortedTasks = new ArrayDeque<>();
        Collection<Joblet> joblets = ncs.getJobletMap().values();
        // TODO(mblow): should we have one jobletmap per cc?
        joblets.stream().filter(joblet -> joblet.getJobId().getCcId().equals(ccId)).forEach(joblet -> {
            joblet.getTaskMap().values().forEach(task -> {
                task.abort();
                abortedTasks.add(task);
            });
            final JobId jobId = joblet.getJobId();
            if (resultPartitionManager != null) {
                resultPartitionManager.abortReader(jobId);
                resultPartitionManager.sweep(jobId);
            }
            ncs.getWorkQueue().schedule(new CleanupJobletWork(ncs, jobId, JobStatus.FAILURE));
        });
        ncs.getExecutor().submit(new EnsureAllCcTasksCompleted(ncs, ccId, abortedTasks));
    }
}
