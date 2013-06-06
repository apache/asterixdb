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

import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobId;

public final class ResultSetPartitionId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final JobId jobId;

    private final ResultSetId resultSetId;
    
    private final int partition;

    public ResultSetPartitionId(JobId jobId, ResultSetId resultSetId, int partition) {
        this.jobId = jobId;
        this.resultSetId = resultSetId;
        this.partition = partition;
    }

    public JobId getJobId() {
        return jobId;
    }

    public ResultSetId getResultSetId() {
        return resultSetId;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((resultSetId == null) ? 0 : resultSetId.hashCode());
        result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
        result = prime * result + partition;
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
        ResultSetPartitionId other = (ResultSetPartitionId) obj;
        if (resultSetId == null) {
            if (other.resultSetId != null)
                return false;
        } else if (!resultSetId.equals(other.resultSetId))
            return false;
        if (jobId == null) {
            if (other.jobId != null)
                return false;
        } else if (!jobId.equals(other.jobId))
            return false;
        if (partition != other.partition)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return jobId.toString() + ":" + resultSetId + ":" + partition;
    }
}