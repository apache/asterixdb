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
package org.apache.asterix.app.result;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.IResultSetReader;
import org.apache.hyracks.api.result.ResultJobRecord.Status;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;

public class ResultReader {
    private IResultSetReader reader;

    private IFrameTupleAccessor frameTupleAccessor;

    // Number of parallel result reader buffers
    public static final int NUM_READERS = 1;

    public ResultReader(IResultSet resultSet, JobId jobId, ResultSetId resultSetId) throws HyracksDataException {
        reader = resultSet.createReader(jobId, resultSetId);
        frameTupleAccessor = new ResultFrameTupleAccessor();
    }

    public Status getStatus() {
        return reader.getResultStatus();
    }

    public int read(IFrame frame) throws HyracksDataException {
        return reader.read(frame);
    }

    public IFrameTupleAccessor getFrameTupleAccessor() {
        return frameTupleAccessor;
    }

    public IResultMetadata getMetadata() {
        return reader.getResultMetadata();
    }
}
