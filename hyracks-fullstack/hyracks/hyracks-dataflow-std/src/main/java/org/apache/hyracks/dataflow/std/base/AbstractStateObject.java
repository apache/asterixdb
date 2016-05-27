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
package org.apache.hyracks.dataflow.std.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.job.JobId;

public abstract class AbstractStateObject implements IStateObject {
    protected JobId jobId;

    protected Object id;

    protected long memoryOccupancy;

    protected AbstractStateObject() {
    }

    protected AbstractStateObject(JobId jobId, Object id) {
        this.jobId = jobId;
        this.id = id;
    }

    @Override
    public final JobId getJobId() {
        return jobId;
    }

    public final void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    @Override
    public final Object getId() {
        return id;
    }

    public final void setId(Object id) {
        this.id = id;
    }

    @Override
    public final long getMemoryOccupancy() {
        return memoryOccupancy;
    }

    public void setMemoryOccupancy(long memoryOccupancy) {
        this.memoryOccupancy = memoryOccupancy;
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
    }
}
