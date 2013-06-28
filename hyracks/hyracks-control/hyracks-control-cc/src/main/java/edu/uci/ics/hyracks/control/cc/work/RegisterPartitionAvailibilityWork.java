/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.work;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.partitions.PartitionMatchMaker;
import edu.uci.ics.hyracks.control.cc.partitions.PartitionUtils;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;

public class RegisterPartitionAvailibilityWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private final PartitionDescriptor partitionDescriptor;

    public RegisterPartitionAvailibilityWork(ClusterControllerService ccs, PartitionDescriptor partitionDescriptor) {
        this.ccs = ccs;
        this.partitionDescriptor = partitionDescriptor;
    }

    @Override
    public void run() {
        final PartitionId pid = partitionDescriptor.getPartitionId();
        JobRun run = ccs.getActiveRunMap().get(pid.getJobId());
        if (run == null) {
            return;
        }
        PartitionMatchMaker pmm = run.getPartitionMatchMaker();
        List<Pair<PartitionDescriptor, PartitionRequest>> matches = pmm
                .registerPartitionDescriptor(partitionDescriptor);
        for (Pair<PartitionDescriptor, PartitionRequest> match : matches) {
            try {
                PartitionUtils.reportPartitionMatch(ccs, pid, match);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return "PartitionAvailable@" + partitionDescriptor;
    }
}