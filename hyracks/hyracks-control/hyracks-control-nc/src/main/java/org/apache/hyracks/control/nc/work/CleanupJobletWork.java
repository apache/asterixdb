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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.IPartition;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;

public class CleanupJobletWork extends AbstractWork {
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
    public void run() {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Cleaning up after job: " + jobId);
        }
        final List<IPartition> unregisteredPartitions = new ArrayList<IPartition>();
        ncs.getPartitionManager().unregisterPartitions(jobId, unregisteredPartitions);
        ncs.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for (IPartition p : unregisteredPartitions) {
                    p.deallocate();
                }
            }
        });
        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        Joblet joblet = jobletMap.remove(jobId);
        if (joblet != null) {
            joblet.cleanup(status);
        }
    }
}