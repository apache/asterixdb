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
package org.apache.hyracks.control.cc;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class PreDistributedJobStore {

    private final Map<JobId, PreDistributedJobDescriptor> preDistributedJobDescriptorMap;

    public PreDistributedJobStore() {
        preDistributedJobDescriptorMap = new Hashtable<>();
    }

    public void addDistributedJobDescriptor(JobId jobId, ActivityClusterGraph activityClusterGraph,
            JobSpecification jobSpecification, Set<Constraint> activityClusterGraphConstraints)
                    throws HyracksException {
        if (preDistributedJobDescriptorMap.get(jobId) != null) {
            throw HyracksException.create(ErrorCode.DUPLICATE_DISTRIBUTED_JOB, jobId);
        }
        PreDistributedJobDescriptor descriptor =
                new PreDistributedJobDescriptor(activityClusterGraph, jobSpecification, activityClusterGraphConstraints);
        preDistributedJobDescriptorMap.put(jobId, descriptor);
    }

    public void checkForExistingDistributedJobDescriptor(JobId jobId) throws HyracksException {
        if (preDistributedJobDescriptorMap.get(jobId) != null) {
            throw HyracksException.create(ErrorCode.DUPLICATE_DISTRIBUTED_JOB, jobId);
        }
    }

    public PreDistributedJobDescriptor getDistributedJobDescriptor(JobId jobId) throws HyracksException {
        PreDistributedJobDescriptor descriptor = preDistributedJobDescriptorMap.get(jobId);
        if (descriptor == null) {
            throw HyracksException.create(ErrorCode.ERROR_FINDING_DISTRIBUTED_JOB, jobId);
        }
        return descriptor;
    }

    public void removeDistributedJobDescriptor(JobId jobId) throws HyracksException {
        PreDistributedJobDescriptor descriptor = preDistributedJobDescriptorMap.get(jobId);
        if (descriptor == null) {
            throw HyracksException.create(ErrorCode.ERROR_FINDING_DISTRIBUTED_JOB, jobId);
        }
        preDistributedJobDescriptorMap.remove(jobId);
    }

    public class PreDistributedJobDescriptor {

        private final ActivityClusterGraph activityClusterGraph;

        private final JobSpecification jobSpecification;

        private final Set<Constraint> activityClusterGraphConstraints;

        private PreDistributedJobDescriptor(ActivityClusterGraph activityClusterGraph,
                JobSpecification jobSpecification, Set<Constraint> activityClusterGraphConstraints) {
            this.activityClusterGraph = activityClusterGraph;
            this.jobSpecification = jobSpecification;
            this.activityClusterGraphConstraints = activityClusterGraphConstraints;
        }

        public ActivityClusterGraph getActivityClusterGraph() {
            return activityClusterGraph;
        }

        public JobSpecification getJobSpecification() {
            return jobSpecification;
        }

        public Set<Constraint> getActivityClusterGraphConstraints() {
            return activityClusterGraphConstraints;
        }
    }
}
