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
package org.apache.asterix.replication.logging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.replication.functions.AsterixReplicationProtocol;
import org.apache.asterix.replication.management.NetworkingUtil;
import org.apache.asterix.replication.management.ReplicationManager;

public class ReplicationLogBuffer {
    private final int logBufferSize;
    private final AtomicBoolean full;
    private int appendOffset;
    private int flushOffset;
    private final ByteBuffer appendBuffer;
    private final ByteBuffer flushBuffer;
    private boolean stop;
    private Map<String, SocketChannel> replicaSockets;
    private ReplicationManager replicationManager;

    public ReplicationLogBuffer(ReplicationManager replicationManager, int logBufferSize) {
        this.replicationManager = replicationManager;
        this.logBufferSize = logBufferSize;
        appendBuffer = ByteBuffer.allocate(logBufferSize);
        flushBuffer = appendBuffer.duplicate();
        full = new AtomicBoolean(false);
        appendOffset = 0;
        flushOffset = 0;
    }

    public void append(ILogRecord logRecord) {
        appendBuffer.putInt(AsterixReplicationProtocol.ReplicationRequestType.REPLICATE_LOG.ordinal());
        appendBuffer.putInt(logRecord.getSerializedLogSize());
        appendBuffer.put(logRecord.getSerializedLog());

        synchronized (this) {
            appendOffset += getLogReplicationSize(logRecord);
            this.notify();
        }
    }

    public void setReplicationSockets(Map<String, SocketChannel> replicaSockets) {
        this.replicaSockets = replicaSockets;
    }

    public synchronized void isFull(boolean full) {
        this.full.set(full);
        this.notify();
    }

    public boolean hasSpace(ILogRecord logRecord) {
        return appendOffset + getLogReplicationSize(logRecord) <= logBufferSize;
    }

    private static int getLogReplicationSize(ILogRecord logRecord) {
        //request type + request length + serialized log length
        return Integer.BYTES + Integer.BYTES + logRecord.getSerializedLogSize();
    }

    public void reset() {
        appendBuffer.position(0);
        appendBuffer.limit(logBufferSize);
        flushBuffer.position(0);
        flushBuffer.limit(logBufferSize);
        full.set(false);
        appendOffset = 0;
        flushOffset = 0;
    }

    public void flush() {
        int endOffset;
        while (!full.get()) {
            synchronized (this) {
                if (appendOffset - flushOffset == 0 && !full.get()) {
                    try {
                        if (stop) {
                            break;
                        }
                        this.wait();
                    } catch (InterruptedException e) {
                        continue;
                    }
                }
                endOffset = appendOffset;
            }
            internalFlush(flushOffset, endOffset);
        }

        internalFlush(flushOffset, appendOffset);
    }

    private void internalFlush(int beginOffset, int endOffset) {
        if (endOffset > beginOffset) {
            int begingPos = flushBuffer.position();
            flushBuffer.limit(endOffset);
            sendRequest(replicaSockets, flushBuffer);
            flushBuffer.position(begingPos + (endOffset - beginOffset));
            flushOffset = endOffset;
        }
    }

    private void sendRequest(Map<String, SocketChannel> replicaSockets, ByteBuffer requestBuffer) {
        Iterator<Map.Entry<String, SocketChannel>> iterator = replicaSockets.entrySet().iterator();
        int begin = requestBuffer.position();
        while (iterator.hasNext()) {
            Entry<String, SocketChannel> replicaSocket = iterator.next();
            SocketChannel clientSocket = replicaSocket.getValue();
            try {
                NetworkingUtil.transferBufferToChannel(clientSocket, requestBuffer);
            } catch (IOException e) {
                if (clientSocket.isOpen()) {
                    try {
                        clientSocket.close();
                    } catch (IOException e2) {
                        e2.printStackTrace();
                    }
                }
                replicationManager.reportFailedReplica(replicaSocket.getKey());
                iterator.remove();
            } finally {
                requestBuffer.position(begin);
            }
        }

    }

    public boolean isStop() {
        return stop;
    }

    public void isStop(boolean stop) {
        this.stop = stop;
    }
}
