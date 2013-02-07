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
package edu.uci.ics.hyracks.client.dataset;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.IDatasetInputChannelMonitor;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetDirectoryServiceConnection;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.client.net.ClientNetworkManager;
import edu.uci.ics.hyracks.comm.channels.DatasetNetworkInputChannel;

// TODO(madhusudancs): Should this implementation be moved to edu.uci.ics.hyracks.client?
public class HyracksDataset implements IHyracksDataset {
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

    public HyracksDataset(IHyracksClientConnection hcc, DatasetClientContext datasetClientCtx, int nReaders)
            throws Exception {
        NetworkAddress ddsAddress = hcc.getDatasetDirectoryServiceInfo();
        datasetDirectoryServiceConnection = new HyracksDatasetDirectoryServiceConnection(new String(
                ddsAddress.getIpAddress()), ddsAddress.getPort());

        netManager = new ClientNetworkManager(nReaders);

        this.datasetClientCtx = datasetClientCtx;

        knownRecords = null;
        monitors = null;
        lastReadPartition = -1;
        lastMonitor = null;
        resultChannel = null;
    }

    @Override
    public void open(JobId jobId, ResultSetId resultSetId) throws IOException {
        this.jobId = jobId;
        this.resultSetId = resultSetId;
        netManager.start();
    }

    @Override
    public byte[] getSerializedRecordDescriptor() {
        byte[] serializedRecordDescriptor = null;
        try {
            serializedRecordDescriptor = datasetDirectoryServiceConnection.getDatasetSerializedRecordDescriptor(jobId,
                    resultSetId);
        } catch (Exception e) {
            // TODO(madhusudancs): Decide what to do in case of error
        }
        return serializedRecordDescriptor;
    }

    @Override
    public int read(ByteBuffer buffer) throws HyracksDataException {
        ByteBuffer readBuffer;
        int readSize = 0;

        if (lastReadPartition == -1) {
            while (knownRecords == null || knownRecords[0] == null) {
                try {
                    knownRecords = datasetDirectoryServiceConnection.getDatasetResultLocationsFunction(jobId,
                            resultSetId, knownRecords);
                    lastReadPartition = 0;
                    resultChannel = new DatasetNetworkInputChannel(netManager,
                            getSocketAddress(knownRecords[lastReadPartition]), jobId, lastReadPartition,
                            NUM_READ_BUFFERS);
                    lastMonitor = getMonitor(lastReadPartition);
                    resultChannel.open(datasetClientCtx);
                    resultChannel.registerMonitor(lastMonitor);
                } catch (HyracksException e) {
                    throw new HyracksDataException(e);
                } catch (UnknownHostException e) {
                    throw new HyracksDataException(e);
                } catch (Exception e) {
                    // Do nothing here.
                }
            }
        }

        while (readSize <= 0 && !((lastReadPartition == knownRecords.length - 1) && (lastMonitor.eosReached()))) {
            while (lastMonitor.getNFramesAvailable() <= 0 && !lastMonitor.eosReached()) {
                synchronized (lastMonitor) {
                    try {
                        lastMonitor.wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            if (lastMonitor.eosReached()) {
                if ((lastReadPartition == knownRecords.length - 1)) {
                    break;
                } else {
                    try {
                        lastReadPartition++;
                        while (knownRecords[lastReadPartition] == null) {
                            try {
                                knownRecords = datasetDirectoryServiceConnection.getDatasetResultLocationsFunction(
                                        jobId, resultSetId, knownRecords);
                            } catch (Exception e) {
                                // Do nothing here.
                            }
                        }

                        resultChannel = new DatasetNetworkInputChannel(netManager,
                                getSocketAddress(knownRecords[lastReadPartition]), jobId, lastReadPartition,
                                NUM_READ_BUFFERS);
                        lastMonitor = getMonitor(lastReadPartition);
                        resultChannel.open(datasetClientCtx);
                        resultChannel.registerMonitor(lastMonitor);
                    } catch (HyracksException e) {
                        throw new HyracksDataException(e);
                    } catch (UnknownHostException e) {
                        throw new HyracksDataException(e);
                    }
                }
            } else {
                readBuffer = resultChannel.getNextBuffer();
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
    }
}
