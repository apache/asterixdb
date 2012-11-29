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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.IDatasetInputChannelMonitor;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetDirectoryServiceConnection;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.client.net.ClientNetworkManager;
import edu.uci.ics.hyracks.comm.channels.DatasetNetworkInputChannel;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

// TODO(madhusudancs): Should this implementation be moved to edu.uci.ics.hyracks.client?
public class HyracksDataset implements IHyracksDataset {
    private final JobId jobId;

    private final List<ResultSetId> rsIds;

    private final IHyracksDatasetDirectoryServiceConnection datasetDirectoryServiceConnection;

    private final ClientNetworkManager netManager;

    private final DatasetClientContext datasetClientCtx;

    private DatasetDirectoryRecord[] knownRecords;

    private IDatasetInputChannelMonitor[] monitors;

    public HyracksDataset(IHyracksClientConnection hcc, JobSpecification jobSpec, JobId jobId, int nReaders)
            throws Exception {
        this.jobId = jobId;
        this.rsIds = jobSpec.getResultSetIds();

        NetworkAddress ddsAddress = hcc.getDatasetDirectoryServiceInfo(jobId);
        datasetDirectoryServiceConnection = new HyracksDatasetDirectoryServiceConnection(new String(
                ddsAddress.getIpAddress()), ddsAddress.getPort());

        netManager = new ClientNetworkManager(nReaders);

        datasetClientCtx = new DatasetClientContext(jobSpec.getFrameSize());

        knownRecords = null;
        monitors = null;
    }

    private void start() throws IOException {
        netManager.start();
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

    private IDatasetInputChannelMonitor getMontior(int partition) throws HyracksException {
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

    /* TODO(madhusudancs): This method is used purely during development for debugging and must be removed before
     * shipping the code 
     */
    private void printBuffer(ByteBuffer buffer) {
        String delim = ",";
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream di = new DataInputStream(bbis);
        RecordDescriptor recordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(32768, recordDescriptor);

        try {
            frameTupleAccessor.reset(buffer);
            for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                int start = frameTupleAccessor.getTupleStartOffset(tIndex) + frameTupleAccessor.getFieldSlotsLength();
                bbis.setByteBuffer(buffer, start);
                Object[] record = new Object[recordDescriptor.getFieldCount()];
                for (int i = 0; i < record.length; ++i) {
                    Object instance = recordDescriptor.getFields()[i].deserialize(di);
                    if (i == 0) {
                        System.out.write(String.valueOf(instance).getBytes());
                    } else {
                        System.out.write((delim + String.valueOf(instance)).getBytes());
                    }
                }
                System.out.write("\n".getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readResults() throws HyracksDataException {
        ByteBuffer buffer = null;

        if (knownRecords == null) {
            return;
        }
        for (int i = 0; i < knownRecords.length; i++) {
            final DatasetDirectoryRecord addr = knownRecords[i];
            if (addr != null) {
                try {
                    DatasetNetworkInputChannel resultChannel = new DatasetNetworkInputChannel(netManager,
                            getSocketAddress(addr), jobId, i, 100);

                    IDatasetInputChannelMonitor monitor = getMontior(i);
                    resultChannel.open(datasetClientCtx);
                    resultChannel.registerMonitor(monitor);

                    while (!monitor.eosReached()) {
                        synchronized (monitor) {
                            try {
                                monitor.wait();
                            } catch (InterruptedException e) {
                                //
                            }
                        }
                        buffer = resultChannel.getNextBuffer();

                        if (buffer != null) {
                            // TODO(madhusudancs): This is a development time debugging statement and should be removed
                            printBuffer(buffer);

                            resultChannel.recycleBuffer(buffer);
                        }
                    }

                } catch (UnknownHostException e) {
                    throw new HyracksDataException(e);
                } catch (HyracksException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    @Override
    public ByteBuffer getResults() {
        try {
            ResultSetId rsId = rsIds.get(0);
            start();
            while (nullExists(knownRecords)) {
                try {
                    knownRecords = datasetDirectoryServiceConnection.getDatasetResultLocationsFunction(jobId, rsId,
                            knownRecords);
                    readResults();
                } catch (Exception e) {
                    // TODO(madhusudancs) Do something here
                }
            }
        } catch (IOException e) {
            // TODO(madhusudancs): Do something here
        }

        return null;
    }

    private SocketAddress getSocketAddress(DatasetDirectoryRecord addr) throws UnknownHostException {
        NetworkAddress netAddr = addr.getNetworkAddress();
        return new InetSocketAddress(InetAddress.getByAddress(netAddr.getIpAddress()), netAddr.getPort());
    }
}
