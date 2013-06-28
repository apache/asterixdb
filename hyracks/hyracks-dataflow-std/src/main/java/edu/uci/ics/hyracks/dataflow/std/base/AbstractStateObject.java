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
package edu.uci.ics.hyracks.dataflow.std.base;

import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.job.JobId;

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
}