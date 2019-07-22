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
package org.apache.hyracks.net.protocols.muxdemux;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.comm.IChannelReadInterface;
import org.apache.hyracks.api.comm.IChannelWriteInterface;
import org.apache.hyracks.api.exceptions.NetException;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.net.protocols.muxdemux.MultiplexedConnection.WriterState;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Handle to a channel that represents a logical full-duplex communication end-point.
 *
 * @author vinayakb
 */
public class ChannelControlBlock implements IChannelControlBlock {
    private final ChannelSet cSet;

    private final int channelId;

    private final IChannelReadInterface ri;

    private final IChannelWriteInterface wi;

    private final AtomicBoolean localClose;

    private final AtomicBoolean localCloseAck;

    private final AtomicBoolean remoteClose;

    private final AtomicBoolean remoteCloseAck;

    ChannelControlBlock(ChannelSet cSet, int channelId, IChannelInterfaceFactory interfaceFactory) {
        this.cSet = cSet;
        this.channelId = channelId;
        localClose = new AtomicBoolean();
        localCloseAck = new AtomicBoolean();
        remoteClose = new AtomicBoolean();
        remoteCloseAck = new AtomicBoolean();
        this.ri = interfaceFactory.createReadInterface(this);
        this.wi = interfaceFactory.createWriteInterface(this);
    }

    @Override
    public int getChannelId() {
        return channelId;
    }

    @Override
    public IChannelReadInterface getReadInterface() {
        return ri;
    }

    @Override
    public IChannelWriteInterface getWriteInterface() {
        return wi;
    }

    synchronized void write(WriterState writerState) throws NetException {
        wi.write(writerState);
    }

    @Override
    public synchronized void writeComplete() {
        wi.writeComplete();
    }

    synchronized int read(ISocketChannel sc, int size) throws IOException, NetException {
        return ri.read(sc, size);
    }

    int getReadCredits() {
        return ri.getCredits();
    }

    void setReadCredits(int credits) {
        ri.setReadCredits(credits);
    }

    @Override
    public synchronized void addWriteCredits(int delta) {
        wi.addCredits(delta);
        wi.adjustChannelWritability();
    }

    synchronized void reportRemoteEOS() {
        ri.flush();
        ri.getFullBufferAcceptor().close();
        remoteClose.set(true);
    }

    void reportRemoteEOSAck() {
        remoteCloseAck.set(true);
    }

    boolean getRemoteEOS() {
        return remoteClose.get();
    }

    void reportLocalEOSAck() {
        localCloseAck.set(true);
    }

    void reportRemoteError(int ecode) {
        ri.getFullBufferAcceptor().error(ecode);
        remoteClose.set(true);
    }

    boolean completelyClosed() {
        return localCloseAck.get() && remoteCloseAck.get();
    }

    @Override
    public boolean isRemotelyClosed() {
        return remoteCloseAck.get();
    }

    @Override
    public void reportLocalEOS() {
        localClose.set(true);
    }

    @Override
    public void addPendingCredits(int credit) {
        cSet.addPendingCredits(channelId, credit);
    }

    @Override
    public void unmarkPendingWrite() {
        cSet.unmarkPendingWrite(channelId);
    }

    @Override
    public void markPendingWrite() {
        cSet.markPendingWrite(channelId);
    }

    @Override
    public String toString() {
        return "Channel:" + channelId + "[localClose: " + localClose + " localCloseAck: " + localCloseAck
                + " remoteClose: " + remoteClose + " remoteCloseAck:" + remoteCloseAck + " readCredits: "
                + ri.getCredits() + " writeCredits: " + wi.getCredits() + "]";
    }

    public InetSocketAddress getRemoteAddress() {
        return cSet.getMultiplexedConnection().getRemoteAddress();
    }

    public JsonNode getState() {
        final ObjectNode state = JSONUtil.createObject();
        state.put("id", channelId);
        state.put("localClose", localClose.get());
        state.put("localCloseAck", localCloseAck.get());
        state.put("remoteClose", remoteClose.get());
        state.put("remoteCloseAck", remoteCloseAck.get());
        state.put("readCredits", ri.getCredits());
        state.put("writeCredits", wi.getCredits());
        state.put("completelyClosed", completelyClosed());
        return state;
    }
}
