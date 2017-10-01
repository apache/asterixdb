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

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;

public class AbortAllJobsWork extends SynchronizableWork {

    private static final Logger LOGGER = Logger.getLogger(AbortAllJobsWork.class.getName());
    private final NodeControllerService ncs;

    public AbortAllJobsWork(NodeControllerService ncs) {
        this.ncs = ncs;
    }

    @Override
    protected void doRun() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Aborting all tasks");
        }
        IDatasetPartitionManager dpm = ncs.getDatasetPartitionManager();
        if (dpm != null) {
            ncs.getDatasetPartitionManager().abortAllReaders();
        } else {
            LOGGER.log(Level.WARNING, "DatasetPartitionManager is null on " + ncs.getId());
        }
        Collection<Joblet> joblets = ncs.getJobletMap().values();
        for (Joblet ji : joblets) {
            Collection<Task> tasks = ji.getTaskMap().values();
            for (Task task : tasks) {
                task.abort();
            }
            ncs.getWorkQueue().schedule(new CleanupJobletWork(ncs, ji.getJobId(), JobStatus.FAILURE));
        }
    }
}
