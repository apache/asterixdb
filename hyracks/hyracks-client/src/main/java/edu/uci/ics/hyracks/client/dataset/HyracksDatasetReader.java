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
package edu.uci.ics.hyracks.client.dataset;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.DatasetJobRecord.Status;
import edu.uci.ics.hyracks.api.dataset.IDatasetInputChannelMonitor;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetDirectoryServiceConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetReader;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.client.net.ClientNetworkManager;
import edu.uci.ics.hyracks.comm.channels.DatasetNetworkInputChannel;

// TODO(madhusudancs): Should this implementation be moved to edu.uci.ics.hyracks.client?
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
            ClientNetworkManager netManager, DatasetClientContext datasetClientCtx, JobId jobId, ResultSetId resultSetId)
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

    @Override
    public int read(ByteBuffer buffer) throws HyracksDataException {
        ByteBuffer readBuffer;
        int readSize = 0;

        if (lastReadPartition == -1) {
            while (knownRecords == null || knownRecords[0] == null) {
                try {
                    knownRecords = datasetDirectoryServiceConnection.getDatasetResultLocations(jobId, resultSetId,
                            knownRecords);
                    lastReadPartition = 0;
                    resultChannel = new DatasetNetworkInputChannel(netManager,
                            getSocketAddress(knownRecords[lastReadPartition]), jobId, resultSetId, lastReadPartition,
                            NUM_READ_BUFFERS);
                    lastMonitor = getMonitor(lastReadPartition);
                    resultChannel.registerMonitor(lastMonitor);
                    resultChannel.open(datasetClientCtx);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }
        }

        while (readSize <= 0 && !(isLastPartitionReadComplete())) {
            synchronized (lastMonitor) {
                while (lastMonitor.getNFramesAvailable() <= 0 && !lastMonitor.eosReached() && !lastMonitor.failed()) {
                    try {
                        lastMonitor.wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            if (lastMonitor.failed()) {
                throw new HyracksDataException("Job Failed.");
            }
            if (isPartitionReadComplete(lastMonitor)) {
                knownRecords[lastReadPartition].readEOS();
                if ((lastReadPartition == knownRecords.length - 1)) {
                    break;
                } else {
                    try {
                        lastReadPartition++;
                        while (knownRecords[lastReadPartition] == null) {
                            knownRecords = datasetDirectoryServiceConnection.getDatasetResultLocations(jobId,
                                    resultSetId, knownRecords);
                        }

                        resultChannel = new DatasetNetworkInputChannel(netManager,
                                getSocketAddress(knownRecords[lastReadPartition]), jobId, resultSetId,
                                lastReadPartition, NUM_READ_BUFFERS);
                        lastMonitor = getMonitor(lastReadPartition);
                        resultChannel.registerMonitor(lastMonitor);
                        resultChannel.open(datasetClientCtx);
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
            } else {
                readBuffer = resultChannel.getNextBuffer();
                lastMonitor.notifyFrameRead();
                if (readBuffer != null) {
                    buffer.put(readBuffer);
                    buffer.flip();
                    readSize = buffer.limit();
                    resultChannel.recycleBuffer(readBuffer);
                }
            }
        }

        return readSize;
    }

    private boolean nullExists(DatasetDirectoryRecord[] locations) {
        if (locations == null) {
            return true;
        }
        for (int i = 0; i < locations.length; i++) {
            if (locations[i] == null) {
                return true;
            }
        }
        return false;
    }

    private boolean isPartitionReadComplete(IDatasetInputChannelMonitor monitor) {
        return (monitor.getNFramesAvailable() <= 0) && (monitor.eosReached());
    }

    private boolean isLastPartitionReadComplete() {
        return ((lastReadPartition == knownRecords.length - 1) && isPartitionReadComplete(lastMonitor));
    }

    private SocketAddress getSocketAddress(DatasetDirectoryRecord addr) throws UnknownHostException {
        NetworkAddress netAddr = addr.getNetworkAddress();
        return new InetSocketAddress(InetAddress.getByAddress(netAddr.getIpAddress()), netAddr.getPort());
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
