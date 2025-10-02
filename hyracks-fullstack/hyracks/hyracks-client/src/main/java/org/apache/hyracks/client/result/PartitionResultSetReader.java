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
package org.apache.hyracks.client.result;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultDirectory;
import org.apache.hyracks.api.result.ResultDirectoryRecord;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.client.net.ClientNetworkManager;

public class PartitionResultSetReader extends ResultSetReader {

    private boolean firstRead;

    public PartitionResultSetReader(IResultDirectory resultDirectory, ClientNetworkManager netManager,
            IHyracksCommonContext resultClientCtx, JobId jobId, ResultSetId resultSetId, int partitionId) {
        super(resultDirectory, netManager, resultClientCtx, jobId, resultSetId);
        this.currentRecord = partitionId;
        this.firstRead = true;
    }

    @Override
    public int read(IFrame frame) throws HyracksDataException {
        frame.reset();
        int readSize = 0;
        try {
            if (isFirstRead() && !hasNextRecord()) {
                return readSize;
            }
            // read until frame is full or all result records have been read
            while (readSize < frame.getFrameSize()) {
                if (currentRecordMonitor.hasMoreFrames()) {
                    final ByteBuffer readBuffer = currentRecordChannel.getNextBuffer();
                    if (readBuffer == null) {
                        throw new IllegalStateException("Unexpected empty frame");
                    }
                    currentRecordMonitor.notifyFrameRead();
                    if (readSize == 0) {
                        final int nBlocks = FrameHelper.deserializeNumOfMinFrame(readBuffer);
                        frame.ensureFrameSize(frame.getMinSize() * nBlocks);
                        frame.getBuffer().clear();
                    }
                    frame.getBuffer().put(readBuffer);
                    currentRecordChannel.recycleBuffer(readBuffer);
                    readSize = frame.getBuffer().position();
                } else {
                    currentRecordChannel.close();
                    if (currentRecordMonitor.failed()) {
                        throw HyracksDataException.create(ErrorCode.FAILED_TO_READ_RESULT, jobId);
                    }
                    break;
                }
            }
            frame.getBuffer().flip();
        } catch (Exception e) {
            if (isLocalFailure()) {
                currentRecordChannel.fail();
            }
            throw e;
        }
        return readSize;
    }

    @Override
    protected boolean hasNextRecord() throws HyracksDataException {
        firstRead = false;
        try {
            knownRecords = resultDirectory.getResultLocations(jobId, resultSetId, knownRecords);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (currentRecord >= knownRecords.length) {
            // exhausted all known records
            return false;
        }
        ResultDirectoryRecord record = getRecord(currentRecord);
        // skip empty records
        if (record.isEmpty()) {
            return false;
        }
        requestRecordData(record);
        return true;
    }

    @Override
    protected boolean isFirstRead() {
        return firstRead;
    }
}
