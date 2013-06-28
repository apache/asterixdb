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
package edu.uci.ics.hyracks.control.common.job.profiling.om;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.MultiResolutionEventProfiler;

public class PartitionProfile implements Serializable {
    private static final long serialVersionUID = 1L;

    private final PartitionId pid;

    private final long openTime;

    private final long closeTime;

    private final MultiResolutionEventProfiler mrep;

    public PartitionProfile(PartitionId pid, long openTime, long closeTime, MultiResolutionEventProfiler mrep) {
        this.pid = pid;
        this.openTime = openTime;
        this.closeTime = closeTime;
        this.mrep = mrep;
    }

    public PartitionId getPartitionId() {
        return pid;
    }

    public long getOpenTime() {
        return openTime;
    }

    public long getCloseTime() {
        return closeTime;
    }

    public MultiResolutionEventProfiler getSamples() {
        return mrep;
    }
}