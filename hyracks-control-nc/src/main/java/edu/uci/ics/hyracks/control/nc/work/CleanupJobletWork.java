/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.nc.work;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class CleanupJobletWork extends SynchronizableWork {
    private static final Logger LOGGER = Logger.getLogger(CleanupJobletWork.class.getName());

    private final NodeControllerService ncs;

    private final JobId jobId;

    private JobStatus status;

    public CleanupJobletWork(NodeControllerService ncs, JobId jobId, JobStatus status) {
        this.ncs = ncs;
        this.jobId = jobId;
        this.status = status;
    }

    @Override
    protected void doRun() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Cleaning up after job: " + jobId);
        }
        ncs.getPartitionManager().unregisterPartitions(jobId);
        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        Joblet joblet = jobletMap.remove(jobId);
        if (joblet != null) {
            joblet.cleanup(status);
        }
        ncs.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    ncs.getClusterController().notifyJobletCleanup(jobId, ncs.getId());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}