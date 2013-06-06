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
package edu.uci.ics.hyracks.control.nc.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.comm.channels.IChannelConnectionFactory;
import edu.uci.ics.hyracks.comm.channels.NetworkOutputChannel;
import edu.uci.ics.hyracks.net.buffers.ICloseableBufferAcceptor;
import edu.uci.ics.hyracks.net.exceptions.NetException;
import edu.uci.ics.hyracks.net.protocols.muxdemux.ChannelControlBlock;
import edu.uci.ics.hyracks.net.protocols.muxdemux.IChannelOpenListener;
import edu.uci.ics.hyracks.net.protocols.muxdemux.MultiplexedConnection;
import edu.uci.ics.hyracks.net.protocols.muxdemux.MuxDemux;
import edu.uci.ics.hyracks.net.protocols.muxdemux.MuxDemuxPerformanceCounters;

public class DatasetNetworkManager implements IChannelConnectionFactory {
    private static final Logger LOGGER = Logger.getLogger(DatasetNetworkManager.class.getName());

    private static final int MAX_CONNECTION_ATTEMPTS = 5;

    static final int INITIAL_MESSAGE_SIZE = 20;

    private final IDatasetPartitionManager partitionManager;

    private final MuxDemux md;

    private NetworkAddress networkAddress;

    public DatasetNetworkManager(InetAddress inetAddress, IDatasetPartitionManager partitionManager, int nThreads)
            throws IOException {
        this.partitionManager = partitionManager;
        md = new MuxDemux(new InetSocketAddress(inetAddress, 0), new ChannelOpenListener(), nThreads,
                MAX_CONNECTION_ATTEMPTS);
    }

    public void start() throws IOException {
        md.start();
        InetSocketAddress sockAddr = md.getLocalAddress();
        networkAddress = new NetworkAddress(sockAddr.getAddress().getAddress(), sockAddr.getPort());
    }

    public NetworkAddress getNetworkAddress() {
        return networkAddress;
    }

    public void stop() {

    }

    public ChannelControlBlock connect(SocketAddress remoteAddress) throws InterruptedException, NetException {
        MultiplexedConnection mConn = md.connect((InetSocketAddress) remoteAddress);
        return mConn.openChannel();
    }

    private class ChannelOpenListener implements IChannelOpenListener {
        @Override
        public void channelOpened(ChannelControlBlock channel) {
            channel.getReadInterface().setFullBufferAcceptor(new InitialBufferAcceptor(channel));
            channel.getReadInterface().getEmptyBufferAcceptor().accept(ByteBuffer.allocate(INITIAL_MESSAGE_SIZE));
        }
    }

    private class InitialBufferAcceptor implements ICloseableBufferAcceptor {
        private final ChannelControlBlock ccb;

        private NetworkOutputChannel noc;

        public InitialBufferAcceptor(ChannelControlBlock ccb) {
            this.ccb = ccb;
        }

        @Override
        public void accept(ByteBuffer buffer) {
            JobId jobId = new JobId(buffer.getLong());
            ResultSetId rsId = new ResultSetId(buffer.getLong());
            int partition = buffer.getInt();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Received initial dataset partition read request for JobId: " + jobId + " partition: "
                        + partition + " on channel: " + ccb);
            }
            noc = new NetworkOutputChannel(ccb, 1);
            try {
                partitionManager.initializeDatasetPartitionReader(jobId, rsId, partition, noc);
            } catch (HyracksException e) {
                noc.abort();
            }
        }

        @Override
        public void close() {

        }

        @Override
        public void error(int ecode) {
            if (noc != null) {
                noc.abort();
            }
        }
    }

    public MuxDemuxPerformanceCounters getPerformanceCounters() {
        return md.getPerformanceCounters();
    }
}