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
package org.apache.hyracks.api.partitions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.io.IWritable;
import org.apache.hyracks.api.job.JobId;

public final class PartitionId implements IWritable, Serializable {
    private static final long serialVersionUID = 1L;

    private JobId jobId;

    private ConnectorDescriptorId cdId;

    private int senderIndex;

    private int receiverIndex;

    public static PartitionId create(DataInput dis) throws IOException {
        PartitionId partitionId = new PartitionId();
        partitionId.readFields(dis);
        return partitionId;
    }

    private PartitionId() {

    }

    public PartitionId(JobId jobId, ConnectorDescriptorId cdId, int senderIndex, int receiverIndex) {
        this.jobId = jobId;
        this.cdId = cdId;
        this.senderIndex = senderIndex;
        this.receiverIndex = receiverIndex;
    }

    public JobId getJobId() {
        return jobId;
    }

    public ConnectorDescriptorId getConnectorDescriptorId() {
        return cdId;
    }

    public int getSenderIndex() {
        return senderIndex;
    }

    public int getReceiverIndex() {
        return receiverIndex;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cdId == null) ? 0 : cdId.hashCode());
        result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
        result = prime * result + receiverIndex;
        result = prime * result + senderIndex;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PartitionId other = (PartitionId) obj;
        if (cdId == null) {
            if (other.cdId != null)
                return false;
        } else if (!cdId.equals(other.cdId))
            return false;
        if (jobId == null) {
            if (other.jobId != null)
                return false;
        } else if (!jobId.equals(other.jobId))
            return false;
        if (receiverIndex != other.receiverIndex)
            return false;
        if (senderIndex != other.senderIndex)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return jobId.toString() + ":" + cdId + ":" + senderIndex + ":" + receiverIndex;
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        cdId.writeFields(output);
        jobId.writeFields(output);
        output.writeInt(receiverIndex);
        output.writeInt(senderIndex);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        cdId = ConnectorDescriptorId.create(input);
        jobId = JobId.create(input);
        receiverIndex = input.readInt();
        senderIndex = input.readInt();
    }
}
