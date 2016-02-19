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
package org.apache.hyracks.control.common.job.profiling.om;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.api.io.IWritable;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.profiling.counters.MultiResolutionEventProfiler;

public class PartitionProfile implements IWritable, Serializable {
    private static final long serialVersionUID = 1L;

    private PartitionId pid;

    private long openTime;

    private long closeTime;

    private MultiResolutionEventProfiler mrep;

    public static PartitionProfile create(DataInput dis) throws IOException {
        PartitionProfile partitionProfile = new PartitionProfile();
        partitionProfile.readFields(dis);
        return partitionProfile;
    }

    private PartitionProfile() {

    }

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

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeLong(closeTime);
        output.writeLong(openTime);
        mrep.writeFields(output);
        pid.writeFields(output);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        closeTime = input.readLong();
        openTime = input.readLong();
        mrep = MultiResolutionEventProfiler.create(input);
        pid = PartitionId.create(input);
    }
}
