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
package org.apache.asterix.app.result;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.asterix.api.common.ResultMetadata;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IJobResultCallback;
import org.apache.hyracks.api.result.ResultJobRecord;
import org.apache.hyracks.api.result.ResultSetMetaData;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.job.profiling.om.JobletProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobResultCallback implements IJobResultCallback {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ICcApplicationContext appCtx;

    public JobResultCallback(ICcApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    @Override
    public void completed(JobId jobId, ResultJobRecord resultJobRecord) {
        try {
            updateResultMetadata(jobId, resultJobRecord);
        } catch (Exception e) {
            LOGGER.error("failed to update result metadata for {}", jobId, e);
        }
    }

    private void updateResultMetadata(JobId jobId, ResultJobRecord resultJobRecord) {
        final ResultSetMetaData resultSetMetaData = resultJobRecord.getResultSetMetaData();
        if (resultSetMetaData == null) {
            return;
        }
        final ResultMetadata metadata = (ResultMetadata) resultSetMetaData.getMetadata();
        metadata.setJobDuration(resultJobRecord.getJobDuration());
        aggregateJobStats(jobId, metadata);
    }

    private void aggregateJobStats(JobId jobId, ResultMetadata metadata) {
        long processedObjects = 0;
        long diskIoCount = 0;
        long aggregateTotalWarningsCount = 0;
        Set<Warning> AggregateWarnings = new HashSet<>();
        IJobManager jobManager =
                ((ClusterControllerService) appCtx.getServiceContext().getControllerService()).getJobManager();
        final JobRun run = jobManager.get(jobId);
        if (run != null) {
            final JobProfile jobProfile = run.getJobProfile();
            final Collection<JobletProfile> jobletProfiles = jobProfile.getJobletProfiles().values();
            final long maxWarnings = run.getJobSpecification().getMaxWarnings();
            for (JobletProfile jp : jobletProfiles) {
                final Collection<TaskProfile> jobletTasksProfile = jp.getTaskProfiles().values();
                for (TaskProfile tp : jobletTasksProfile) {
                    processedObjects += tp.getStatsCollector().getAggregatedStats().getTupleCounter().get();
                    diskIoCount += tp.getStatsCollector().getAggregatedStats().getDiskIoCounter().get();
                    aggregateTotalWarningsCount += tp.getTotalWarningsCount();
                    Set<Warning> taskWarnings = tp.getWarnings();
                    if (AggregateWarnings.size() < maxWarnings && !taskWarnings.isEmpty()) {
                        Iterator<Warning> taskWarningsIt = taskWarnings.iterator();
                        while (AggregateWarnings.size() < maxWarnings && taskWarningsIt.hasNext()) {
                            AggregateWarnings.add(taskWarningsIt.next());
                        }
                    }
                }
            }
        }
        metadata.setProcessedObjects(processedObjects);
        metadata.setWarnings(AggregateWarnings);
        metadata.setDiskIoCount(diskIoCount);
        metadata.setTotalWarningsCount(aggregateTotalWarningsCount);
        if (run != null && run.getFlags() != null && run.getFlags().contains(JobFlag.PROFILE_RUNTIME)) {
            metadata.setJobProfile(run.getJobProfile().toJSON());
        } else {
            metadata.setJobProfile(null);
        }
    }
}
