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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.channels.IInputChannelMonitor;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultDirectory;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.IResultSetReader;
import org.apache.hyracks.api.result.ResultDirectoryRecord;
import org.apache.hyracks.api.result.ResultJobRecord.Status;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.client.net.ClientNetworkManager;
import org.apache.hyracks.comm.channels.ResultNetworkInputChannel;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@NotThreadSafe
public class ResultSetReader implements IResultSetReader {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int NUM_READ_BUFFERS = 1;
    private final IResultDirectory resultDirectory;
    private final ClientNetworkManager netManager;
    private final IHyracksCommonContext resultClientCtx;
    private final JobId jobId;
    private final ResultSetId resultSetId;
    private ResultDirectoryRecord[] knownRecords;
    private ResultInputChannelMonitor[] monitors;
    private ResultInputChannelMonitor currentRecordMonitor;
    private ResultNetworkInputChannel currentRecordChannel;
    private int currentRecord;

    public ResultSetReader(IResultDirectory resultDirectory, ClientNetworkManager netManager,
            IHyracksCommonContext resultClientCtx, JobId jobId, ResultSetId resultSetId) {
        this.resultDirectory = resultDirectory;
        this.netManager = netManager;
        this.resultClientCtx = resultClientCtx;
        this.jobId = jobId;
        this.resultSetId = resultSetId;
        currentRecord = -1;
    }

    @Override
    public Status getResultStatus() {
        try {
            return resultDirectory.getResultStatus(jobId, resultSetId);
        } catch (HyracksDataException e) {
            if (e.getErrorCode() != ErrorCode.NO_RESULT_SET) {
                LOGGER.log(Level.WARN, "Exception retrieving result set for job " + jobId, e);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Exception retrieving result set for job " + jobId, e);
        }
        return null;
    }

    @Override
    public int read(IFrame frame) throws HyracksDataException {
        frame.reset();
        int readSize = 0;
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
                if (isLastRecord() || !hasNextRecord()) {
                    break;
                }
            }
        }
        frame.getBuffer().flip();
        return readSize;
    }

    @Override
    public IResultMetadata getResultMetadata() {
        try {
            return resultDirectory.getResultMetadata(jobId, resultSetId);
        } catch (HyracksDataException e) {
            if (e.getErrorCode() != ErrorCode.NO_RESULT_SET) {
                LOGGER.log(Level.WARN, "Exception retrieving result set for job " + jobId, e);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Exception retrieving result set for job " + jobId, e);
        }
        return null;
    }

    private SocketAddress getSocketAddress(ResultDirectoryRecord record) throws HyracksDataException {
        try {
            final NetworkAddress netAddr = record.getNetworkAddress();
            return new InetSocketAddress(InetAddress.getByAddress(netAddr.lookupIpAddress()), netAddr.getPort());
        } catch (UnknownHostException e) {
            throw HyracksDataException.create(e);
        }
    }

    private ResultInputChannelMonitor getMonitor(int partition) {
        if (knownRecords == null || knownRecords[partition] == null) {
            throw new IllegalStateException("Accessing monitors before obtaining the corresponding addresses");
        }
        if (monitors == null) {
            monitors = new ResultInputChannelMonitor[knownRecords.length];
        }
        if (monitors[partition] == null) {
            monitors[partition] = new ResultInputChannelMonitor();
        }
        return monitors[partition];
    }

    private boolean hasNextRecord() throws HyracksDataException {
        currentRecord++;
        ResultDirectoryRecord record = getRecord(currentRecord);
        // skip empty records
        while (record.isEmpty() && ++currentRecord < knownRecords.length) {
            record = getRecord(currentRecord);
        }
        if (currentRecord == knownRecords.length) {
            // exhausted all known records
            return false;
        }
        requestRecordData(record);
        return true;
    }

    private ResultDirectoryRecord getRecord(int recordNum) throws HyracksDataException {
        try {
            while (knownRecords == null || knownRecords[recordNum] == null) {
                knownRecords = resultDirectory.getResultLocations(jobId, resultSetId, knownRecords);
            }
            return knownRecords[recordNum];
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void requestRecordData(ResultDirectoryRecord record) throws HyracksDataException {
        currentRecordChannel = new ResultNetworkInputChannel(netManager, getSocketAddress(record), jobId, resultSetId,
                currentRecord, NUM_READ_BUFFERS);
        currentRecordMonitor = getMonitor(currentRecord);
        currentRecordChannel.registerMonitor(currentRecordMonitor);
        currentRecordChannel.open(resultClientCtx);
    }

    private boolean isFirstRead() {
        return currentRecord == -1;
    }

    private boolean isLastRecord() {
        return knownRecords != null && currentRecord == knownRecords.length - 1;
    }

    private static class ResultInputChannelMonitor implements IInputChannelMonitor {

        private int availableFrames;
        private boolean eos;
        private boolean failed;

        ResultInputChannelMonitor() {
            eos = false;
            failed = false;
        }

        @Override
        public synchronized void notifyFailure(IInputChannel channel, int errorCode) {
            failed = true;
            notifyAll();
        }

        @Override
        public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
            availableFrames += nFrames;
            notifyAll();
        }

        @Override
        public synchronized void notifyEndOfStream(IInputChannel channel) {
            eos = true;
            notifyAll();
        }

        synchronized boolean failed() {
            return failed;
        }

        synchronized void notifyFrameRead() {
            availableFrames--;
            notifyAll();
        }

        synchronized boolean hasMoreFrames() throws HyracksDataException {
            while (!failed && !eos && availableFrames == 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
            }
            return !failed && !isFullyConsumed();
        }

        private synchronized boolean isFullyConsumed() {
            return availableFrames == 0 && eos;
        }
    }
}
