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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;

public class GraceHashJoinPartitionState extends AbstractStateObject {
    private RunFileWriter[] fWriters;

    public GraceHashJoinPartitionState(JobId jobId, Object id) {
        super(jobId, id);
    }

    public RunFileWriter[] getRunWriters() {
        return fWriters;
    }

    public void setRunWriters(RunFileWriter[] fWriters) {
        this.fWriters = fWriters;
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {

    }

    @Override
    public void fromBytes(DataInput in) throws IOException {

    }
}