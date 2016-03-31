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
package org.apache.hyracks.client.dataset;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataset.DatasetDirectoryRecord;
import org.apache.hyracks.api.dataset.DatasetJobRecord.Status;
import org.apache.hyracks.api.dataset.IDatasetInputChannelMonitor;
import org.apache.hyracks.api.dataset.IHyracksDatasetDirectoryServiceConnection;
import org.apache.hyracks.api.dataset.IHyracksDatasetReader;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.client.net.ClientNetworkManager;
import org.apache.hyracks.comm.channels.DatasetNetworkInputChannel;

// TODO(madhusudancs): Should this implementation be moved to org.apache.hyracks.client?
public class HyracksDatasetReader implements IHyracksDatasetReader {
    private static final Logger LOGGER = Logger.getLogger(HyracksDatasetReader.class.getName());

    private final IHyracksDatasetDirectoryServiceConnection datasetDirectoryServiceConnection;

    private final ClientNetworkManager netManager;

    private final DatasetClientContext datasetClientCtx;

    private JobId jobId;

    private ResultSetId resultSetId;

    private DatasetDirectoryRecord[] knownRecords;

    private IDatasetInputChannelMonitor[] monitors;

    private int lastReadPartition;

    private IDatasetInputChannelMonitor lastMonitor;

    private DatasetNetworkInputChannel resultChannel;

    private static int NUM_READ_BUFFERS = 1;

    public HyracksDatasetReader(IHyracksDatasetDirectoryServiceConnection datasetDirectoryServiceConnection,
            ClientNetworkManager netManager, DatasetClientContext datasetClientCtx, JobId jobId,
            ResultSetId resultSetId)
            throws Exception {
        this.datasetDirectoryServiceConnection = datasetDirectoryServiceConnection;
        this.netManager = netManager;
        this.datasetClientCtx = datasetClientCtx;
        this.jobId = jobId;
        this.resultSetId = resultSetId;
        knownRecords = null;
        monitors = null;
        lastReadPartition = -1;
        lastMonitor = null;
        resultChannel = null;
    }

    @Override
    public Status getResultStatus() {
        Status status = null;
        try {
            status = datasetDirectoryServiceConnection.getDatasetResultStatus(jobId, resultSetId);
        } catch (Exception e) {
            // TODO(madhusudancs): Decide what to do in case of error
        }
        return status;
    }

    private DatasetDirectoryRecord getRecord(int partition) throws Exception {
        while (knownRecords == null || knownRecords[partition] == null) {
            knownRecords = datasetDirectoryServiceConnection
                    .getDatasetResultLocations(jobId, resultSetId, knownRecords);
        }
        return knownRecords[partition];
    }

    private boolean nextPartition() throws HyracksDataException {
        ++lastReadPartition;
        try {
            DatasetDirectoryRecord record = getRecord(lastReadPartition);
            while (record.getEmpty() && (++lastReadPartition) < knownRecords.length) {
                record = getRecord(lastReadPartition);
            }
            if (lastReadPartition == knownRecords.length) {
                return false;
            }
            resultChannel = new DatasetNetworkInputChannel(netManager, getSocketAddress(record), jobId, resultSetId,
                    lastReadPartition, NUM_READ_BUFFERS);
            lastMonitor = getMonitor(lastReadPartition);
            resultChannel.registerMonitor(lastMonitor);
            resultChannel.open(datasetClientCtx);
            return true;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public int read(IFrame frame) throws HyracksDataException {
        frame.reset();
        ByteBuffer readBuffer;
        int readSize = 0;

        if (lastReadPartition == -1) {
            if (!nextPartition()) {
                return readSize;
            }
        }

        while (readSize < frame.getFrameSize()
                && !((lastReadPartition == knownRecords.length - 1) && isPartitionReadComplete(lastMonitor))) {
            waitForNextFrame(lastMonitor);
            if (isPartitionReadComplete(lastMonitor)) {
                knownRecords[lastReadPartition].readEOS();
                resultChannel.close();
                if ((lastReadPartition == knownRecords.length - 1) || !nextPartition()) {
                    break;
                }
            } else {
                readBuffer = resultChannel.getNextBuffer();
                lastMonitor.notifyFrameRead();
                if (readBuffer != null) {
                    if (readSize <=0) {
                        int nBlocks = FrameHelper.deserializeNumOfMinFrame(readBuffer);
                        frame.ensureFrameSize(frame.getMinSize() * nBlocks);
                        frame.getBuffer().clear();
                        frame.getBuffer().put(readBuffer);
                        resultChannel.recycleBuffer(readBuffer);
                        readSize = frame.getBuffer().position();
                    } else {
                        frame.getBuffer().put(readBuffer);
                        resultChannel.recycleBuffer(readBuffer);
                        readSize = frame.getBuffer().position();
                    }
                }
            }
        }

        frame.getBuffer().flip();
        return readSize;
    }

    private static void waitForNextFrame(IDatasetInputChannelMonitor monitor) throws HyracksDataException {
        synchronized (monitor) {
            while (monitor.getNFramesAvailable() <= 0 && !monitor.eosReached() && !monitor.failed()) {
                try {
                    monitor.wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
        if (monitor.failed()) {
            throw new HyracksDataException("Job Failed.");
        }
    }

    private boolean isPartitionReadComplete(IDatasetInputChannelMonitor monitor) {
        return (monitor.getNFramesAvailable() <= 0) && (monitor.eosReached());
    }

    private SocketAddress getSocketAddress(DatasetDirectoryRecord addr) throws UnknownHostException {
        NetworkAddress netAddr = addr.getNetworkAddress();
        return new InetSocketAddress(InetAddress.getByAddress(netAddr.lookupIpAddress()), netAddr.getPort());
    }

    private IDatasetInputChannelMonitor getMonitor(int partition) throws HyracksException {
        if (knownRecords == null || knownRecords[partition] == null) {
            throw new HyracksException("Accessing monitors before the obtaining the corresponding addresses.");
        }
        if (monitors == null) {
            monitors = new DatasetInputChannelMonitor[knownRecords.length];
        }
        if (monitors[partition] == null) {
            monitors[partition] = new DatasetInputChannelMonitor();
        }
        return monitors[partition];
    }

    private class DatasetInputChannelMonitor implements IDatasetInputChannelMonitor {
        private final AtomicInteger nAvailableFrames;

        private final AtomicBoolean eos;

        private final AtomicBoolean failed;

        public DatasetInputChannelMonitor() {
            nAvailableFrames = new AtomicInteger(0);
            eos = new AtomicBoolean(false);
            failed = new AtomicBoolean(false);
        }

        @Override
        public synchronized void notifyFailure(IInputChannel channel) {
            failed.set(true);
            notifyAll();
        }

        @Override
        public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
            nAvailableFrames.addAndGet(nFrames);
            notifyAll();
        }

        @Override
        public synchronized void notifyEndOfStream(IInputChannel channel) {
            eos.set(true);
            notifyAll();
        }

        @Override
        public synchronized boolean eosReached() {
            return eos.get();
        }

        @Override
        public synchronized boolean failed() {
            return failed.get();
        }

        @Override
        public synchronized int getNFramesAvailable() {
            return nAvailableFrames.get();
        }

        @Override
        public synchronized void notifyFrameRead() {
            nAvailableFrames.decrementAndGet();
        }

    }
}
