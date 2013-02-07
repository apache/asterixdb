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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.client.dataset.DatasetClientContext;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ResultReader implements Runnable {
    private final DatasetClientContext datasetClientCtx;

    private final IHyracksDataset hyracksDataset;

    private final PrintWriter responseWriter;

    private final ByteBufferInputStream bbis;

    private final DataInputStream di;

    private final ByteBuffer readBuffer;

    private IDataFormat format;

    private int queryCount;

    private JobId jobId;

    private ResultSetId resultSetId;

    private RecordDescriptor recordDescriptor;

    private FrameTupleAccessor frameTupleAccessor;

    // 32K buffer size;
    private static final int FRAME_SIZE = 32768;

    // Number of parallel result reader buffers
    private static final int NUM_READERS = 1;

    private static final String DELIM = "; ";

    public ResultReader(IHyracksClientConnection hcc, PrintWriter responseWriter) throws Exception {
        datasetClientCtx = new DatasetClientContext(FRAME_SIZE);

        hyracksDataset = new HyracksDataset(hcc, datasetClientCtx, NUM_READERS);

        this.responseWriter = responseWriter;

        bbis = new ByteBufferInputStream();
        di = new DataInputStream(bbis);

        readBuffer = datasetClientCtx.allocateFrame();

        queryCount = 1;
    }

    public void setFormat(IDataFormat format) {
        this.format = format;
    }

    public synchronized void notifyJobStart(JobId jobId, ResultSetId rsId) {
        this.jobId = jobId;
        this.resultSetId = rsId;
        notifyAll();
    }

    @Override
    public void run() {
        synchronized (this) {
            while (jobId == null || resultSetId == null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // Do nothing here
                }
            }
        }

        try {
            hyracksDataset.open(jobId, resultSetId);
            byte[] serializedRecordDescriptor = hyracksDataset.getSerializedRecordDescriptor();

            recordDescriptor = (RecordDescriptor) JavaSerializationUtils.deserialize(serializedRecordDescriptor, format
                    .getSerdeProvider().getClass().getClassLoader());

            frameTupleAccessor = new FrameTupleAccessor(datasetClientCtx.getFrameSize(), recordDescriptor);

            int i = 0;

            responseWriter.println("<H1>Result:</H1>");
            responseWriter.println("<PRE>");
            responseWriter.println("Query:" + queryCount++);

            while (true) {
                int size = hyracksDataset.read(readBuffer);

                if (size <= 0) {
                    break;
                }
                i += writeOutputLines(readBuffer);
                if (i > 500) {
                    responseWriter.println("...");
                    responseWriter.println("SKIPPING THE REST OF THE RESULTS");
                    break;
                }
            }

            responseWriter.println();
            responseWriter.println("</PRE>");
        } catch (HyracksDataException e) {
            // TODO(madhusudancs): Do something here
            e.printStackTrace(responseWriter);
        } catch (IOException e) {
            // TODO(madhusudancs): Do something here
            e.printStackTrace(responseWriter);
        } catch (ClassNotFoundException e) {
            // TODO(madhusudancs): Do something here
            e.printStackTrace(responseWriter);
        }
    }

    private int writeOutputLines(ByteBuffer buffer) throws HyracksDataException {
        int lineCount = 0;
        try {
            frameTupleAccessor.reset(buffer);
            for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                int start = frameTupleAccessor.getTupleStartOffset(tIndex) + frameTupleAccessor.getFieldSlotsLength();
                bbis.setByteBuffer(buffer, start);
                Object[] record = new Object[recordDescriptor.getFieldCount()];
                for (int i = 0; i < record.length; ++i) {
                    Object instance = recordDescriptor.getFields()[i].deserialize(di);
                    if (i == 0) {
                        responseWriter.print(String.valueOf(instance));
                    } else {
                        responseWriter.print(DELIM + String.valueOf(instance));
                    }
                }
                responseWriter.println();
                lineCount++;
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return lineCount;
    }
}
