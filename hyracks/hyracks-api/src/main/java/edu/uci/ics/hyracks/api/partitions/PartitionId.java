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
package edu.uci.ics.hyracks.api.partitions;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobId;

public final class PartitionId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final JobId jobId;

    private final ConnectorDescriptorId cdId;

    private final int senderIndex;

    private final int receiverIndex;

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
}