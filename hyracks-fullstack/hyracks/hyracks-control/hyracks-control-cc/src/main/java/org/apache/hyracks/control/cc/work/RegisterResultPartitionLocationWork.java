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
package org.apache.hyracks.control.cc.work;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.common.work.NoOpCallback;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegisterResultPartitionLocationWork extends AbstractWork {

    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;

    private final JobId jobId;

    private final ResultSetId rsId;

    private final IResultMetadata metadata;

    private final boolean emptyResult;

    private final int partition;

    private final int nPartitions;

    private final NetworkAddress networkAddress;

    public RegisterResultPartitionLocationWork(ClusterControllerService ccs, JobId jobId, ResultSetId rsId,
            IResultMetadata metadata, boolean emptyResult, int partition, int nPartitions,
            NetworkAddress networkAddress) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.rsId = rsId;
        this.metadata = metadata;
        this.emptyResult = emptyResult;
        this.partition = partition;
        this.nPartitions = nPartitions;
        this.networkAddress = networkAddress;
    }

    @Override
    public void run() {
        try {
            ccs.getResultDirectoryService().registerResultPartitionLocation(jobId, rsId, metadata, emptyResult,
                    partition, nPartitions, networkAddress);
        } catch (HyracksDataException e) {
            LOGGER.log(Level.WARN, "Failed to register partition location", e);
            // Should fail the job if exists on cc, otherwise, do nothing
            JobRun jobRun = ccs.getJobManager().get(jobId);
            if (jobRun != null) {
                List<Exception> exceptions = new ArrayList<>();
                exceptions.add(e);
                jobRun.getExecutor().abortJob(exceptions, NoOpCallback.INSTANCE);
            }
        }
    }

    @Override
    public String toString() {
        return getName() + ": JobId@" + jobId + " ResultSetId@" + rsId + " Partition@" + partition + " NPartitions@"
                + nPartitions + " ResultPartitionLocation@" + networkAddress + " metadata@" + metadata + " EmptyResult@"
                + emptyResult;
    }
}
