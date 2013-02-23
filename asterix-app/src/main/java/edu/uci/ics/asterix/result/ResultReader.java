/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord.Status;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.client.dataset.DatasetClientContext;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;

public class ResultReader {
    private final DatasetClientContext datasetClientCtx;

    private final IHyracksDataset hyracksDataset;

    private IFrameTupleAccessor frameTupleAccessor;

    // Number of parallel result reader buffers
    private static final int NUM_READERS = 1;

    // 32K buffer size;
    public static final int FRAME_SIZE = 32768;

    public ResultReader(IHyracksClientConnection hcc) throws Exception {
        datasetClientCtx = new DatasetClientContext(FRAME_SIZE);
        hyracksDataset = new HyracksDataset(hcc, datasetClientCtx, NUM_READERS);
    }

    public void open(JobId jobId, ResultSetId resultSetId) throws IOException, ClassNotFoundException {
        hyracksDataset.open(jobId, resultSetId);

        frameTupleAccessor = new ResultFrameTupleAccessor(datasetClientCtx.getFrameSize());
    }

    public Status getStatus() {
        return hyracksDataset.getResultStatus();
    }

    public int read(ByteBuffer buffer) throws HyracksDataException {
        return hyracksDataset.read(buffer);
    }

    public IFrameTupleAccessor getFrameTupleAccessor() {
        return frameTupleAccessor;
    }
}
