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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.partitions.PartitionMatchMaker;
import org.apache.hyracks.control.cc.partitions.PartitionUtils;
import org.apache.hyracks.control.common.job.PartitionDescriptor;
import org.apache.hyracks.control.common.job.PartitionRequest;
import org.apache.hyracks.control.common.work.AbstractWork;

public class RegisterPartitionRequestWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private final PartitionRequest partitionRequest;

    public RegisterPartitionRequestWork(ClusterControllerService ccs, PartitionRequest partitionRequest) {
        this.ccs = ccs;
        this.partitionRequest = partitionRequest;
    }

    @Override
    public void run() {
        PartitionId pid = partitionRequest.getPartitionId();
        IJobManager jobManager = ccs.getJobManager();
        JobRun run = jobManager.get(pid.getJobId());
        if (run == null) {
            return;
        }
        PartitionMatchMaker pmm = run.getPartitionMatchMaker();
        Pair<PartitionDescriptor, PartitionRequest> match = pmm.matchPartitionRequest(partitionRequest);
        if (match != null) {
            try {
                PartitionUtils.reportPartitionMatch(ccs, pid, match);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return getName() + ": " + partitionRequest;
    }
}
