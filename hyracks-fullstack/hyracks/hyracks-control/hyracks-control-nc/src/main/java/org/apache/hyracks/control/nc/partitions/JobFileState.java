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
package org.apache.hyracks.control.nc.partitions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;

public class JobFileState implements IStateObject {

    private final FileReference fileRef;
    private final JobId jobId;
    private final JobFileId jobFileId;

    public JobFileState(FileReference fileRef, JobId jobId, long fileId) {
        this.fileRef = fileRef;
        this.jobFileId = new JobFileId(fileId);
        this.jobId = jobId;
    }

    @Override
    public JobId getJobId() {
        return jobId;
    }

    @Override
    public Object getId() {
        return jobFileId;
    }

    @Override
    public long getMemoryOccupancy() {
        return 0;
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {

    }

    @Override
    public void fromBytes(DataInput in) throws IOException {

    }

    public FileReference getFileRef() {
        return fileRef;
    }

    public static class JobFileId {

        private final long fileId;

        public JobFileId(long fileId) {
            this.fileId = fileId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof JobFileId)) {
                return false;
            }
            JobFileId otherFileId = (JobFileId) o;
            return otherFileId.fileId == fileId;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(fileId);
        }
    }
}
