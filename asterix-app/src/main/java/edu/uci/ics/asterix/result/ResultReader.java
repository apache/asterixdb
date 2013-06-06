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
package edu.uci.ics.asterix.result;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataset.DatasetJobRecord.Status;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetReader;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;

public class ResultReader {
    private final IHyracksDataset hyracksDataset;

    private IHyracksDatasetReader reader;

    private IFrameTupleAccessor frameTupleAccessor;

    // Number of parallel result reader buffers
    public static final int NUM_READERS = 1;

    public static final int FRAME_SIZE = GlobalConfig.getFrameSize();

    public ResultReader(IHyracksClientConnection hcc, IHyracksDataset hdc) throws Exception {
        hyracksDataset = hdc;
    }

    public void open(JobId jobId, ResultSetId resultSetId) throws HyracksDataException {
        reader = hyracksDataset.createReader(jobId, resultSetId);

        frameTupleAccessor = new ResultFrameTupleAccessor(FRAME_SIZE);
    }

    public Status getStatus() {
        return reader.getResultStatus();
    }

    public int read(ByteBuffer buffer) throws HyracksDataException {
        return reader.read(buffer);
    }

    public IFrameTupleAccessor getFrameTupleAccessor() {
        return frameTupleAccessor;
    }
}
