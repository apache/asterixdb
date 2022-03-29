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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.replication.management.LogReplicationManager;

public class ReplicationLogBuffer {

    private final int logBufferSize;
    private final AtomicBoolean full;
    private int appendOffset;
    private int replicationOffset;
    private final ByteBuffer appendBuffer;
    private final ByteBuffer replicationBuffer;
    private boolean stop;
    private final LogReplicationManager replicationManager;
    private final int batchSize;

    public ReplicationLogBuffer(LogReplicationManager replicationManager, int logBufferSize, int batchSize) {
        this.replicationManager = replicationManager;
        this.logBufferSize = logBufferSize;
        this.batchSize = batchSize;
        appendBuffer = ByteBuffer.allocate(logBufferSize);
        replicationBuffer = appendBuffer.duplicate();
        full = new AtomicBoolean(false);
        appendOffset = 0;
        replicationOffset = 0;
    }

    public void append(ILogRecord logRecord) {
        appendBuffer.putInt(logRecord.getRemoteLogSize());
        logRecord.writeRemoteLogRecord(appendBuffer);

        synchronized (this) {
            appendOffset += getLogReplicationSize(logRecord);
            this.notify();
        }
    }

    public synchronized void setFull(boolean full) {
        this.full.set(full);
        this.notify();
    }

    public boolean hasSpace(ILogRecord logRecord) {
        return appendOffset + getLogReplicationSize(logRecord) <= logBufferSize;
    }

    private static int getLogReplicationSize(ILogRecord logRecord) {
        //log length (4 bytes) + remote log size
        return Integer.BYTES + logRecord.getRemoteLogSize();
    }

    public void reset() {
        appendBuffer.position(0);
        appendBuffer.limit(logBufferSize);
        replicationBuffer.position(0);
        replicationBuffer.limit(logBufferSize);
        full.set(false);
        appendOffset = 0;
        replicationOffset = 0;
        stop = false;
    }

    public void flush() {
        int endOffset;
        while (!full.get()) {
            synchronized (this) {
                if (appendOffset - replicationOffset == 0 && !full.get()) {
                    try {
                        if (stop) {
                            break;
                        }
                        this.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        continue;
                    }
                }
                endOffset = appendOffset;
            }
            internalFlush(replicationOffset, endOffset);
        }
        internalFlush(replicationOffset, appendOffset);
    }

    private void internalFlush(int beginOffset, int endOffset) {
        if (endOffset > beginOffset) {
            int begingPos = replicationBuffer.position();
            replicationBuffer.limit(endOffset);
            transferBuffer(replicationBuffer);
            replicationBuffer.position(begingPos + (endOffset - beginOffset));
            replicationOffset = endOffset;
        }
    }

    private void transferBuffer(ByteBuffer buffer) {
        if (buffer.remaining() <= batchSize) {
            //the current batch can be sent as it is
            replicationManager.transferBatch(buffer);
            return;
        }
        /**
         * break the batch into smaller batches
         */
        int totalTransferLimit = buffer.limit();
        while (buffer.hasRemaining()) {
            if (buffer.remaining() > batchSize) {

                //mark the beginning of this batch
                buffer.mark();
                int currentBatchSize = 0;
                while (currentBatchSize < batchSize) {
                    int logSize = replicationBuffer.getInt();
                    //add the size of the log record itself + 4 bytes for its size
                    currentBatchSize += logSize + Integer.BYTES;
                    //go to the beginning of the next log
                    buffer.position(buffer.position() + logSize);
                }
                //set the limit to the end of this batch
                buffer.limit(buffer.position());
                //return to the beginning of the batch position
                buffer.reset();
            }
            replicationManager.transferBatch(buffer);
            //return the original limit to check the new remaining size
            buffer.limit(totalTransferLimit);
        }
    }

    public boolean isStop() {
        return stop;
    }

    public void isStop(boolean stop) {
        this.stop = stop;
    }

    public int getLogBufferSize() {
        return logBufferSize;
    }

    public LogReplicationManager getReplicationManager() {
        return replicationManager;
    }
}
