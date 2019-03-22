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
package org.apache.hyracks.ipc.sockets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;

import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.util.NetworkUtil;
import org.apache.hyracks.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SslSocketChannel implements ISocketChannel {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int DEFAULT_APP_BUFFER_SIZE =
            StorageUtil.getIntSizeInBytes(1, StorageUtil.StorageUnit.MEGABYTE);
    private final SocketChannel socketChannel;
    private final SSLEngine engine;
    private ByteBuffer outEncryptedData;
    private ByteBuffer inAppData;
    private ByteBuffer inEncryptedData;
    private boolean partialRecord = false;
    private boolean cachedData = false;
    private boolean pendingWrite = false;

    public SslSocketChannel(SocketChannel socketChannel, SSLEngine engine) {
        this.socketChannel = socketChannel;
        this.engine = engine;
        inAppData = ByteBuffer.allocate(DEFAULT_APP_BUFFER_SIZE);
        inAppData.limit(0);
        final SSLSession sslSession = engine.getSession();
        inEncryptedData = ByteBuffer.allocate(sslSession.getPacketBufferSize());
        outEncryptedData = ByteBuffer.allocate(sslSession.getPacketBufferSize());
        outEncryptedData.limit(0);
    }

    @Override
    public synchronized boolean handshake() {
        try {
            LOGGER.debug("starting SSL handshake {}", this);
            engine.beginHandshake();
            final SslHandshake sslHandshake = new SslHandshake(this);
            final boolean success = sslHandshake.handshake();
            if (success) {
                LOGGER.debug("SSL handshake successful {}", this);
            }
            return success;
        } catch (Exception e) {
            LOGGER.error("handshake failed {}", this, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean requiresHandshake() {
        return true;
    }

    @Override
    public synchronized int read(ByteBuffer buffer) throws IOException {
        int transfereeBytes = 0;
        if (cachedData) {
            transfereeBytes += transferTo(inAppData, buffer);
        }
        if (buffer.hasRemaining()) {
            if (!partialRecord) {
                inEncryptedData.clear();
            }
            final int bytesRead = socketChannel.read(inEncryptedData);
            if (bytesRead > 0) {
                partialRecord = false;
                inEncryptedData.flip();
                inAppData.clear();
                if (decrypt() > 0) {
                    inAppData.flip();
                    transfereeBytes += transferTo(inAppData, buffer);
                } else {
                    inAppData.limit(0);
                }
            } else if (bytesRead < 0) {
                handleEndOfStreamQuietly();
                return -1;
            }
        }
        cachedData = inAppData.hasRemaining();
        return transfereeBytes;
    }

    private int decrypt() throws IOException {
        int decryptedBytes = 0;
        while (inEncryptedData.hasRemaining() && !partialRecord) {
            SSLEngineResult result = engine.unwrap(inEncryptedData, inAppData);
            switch (result.getStatus()) {
                case OK:
                    decryptedBytes += result.bytesProduced();
                    partialRecord = false;
                    break;
                case BUFFER_OVERFLOW:
                    inAppData = NetworkUtil.enlargeSslApplicationBuffer(engine, inAppData);
                    break;
                case BUFFER_UNDERFLOW:
                    handleReadUnderflow();
                    break;
                case CLOSED:
                    close();
                    return -1;
                default:
                    throw new IllegalStateException("Invalid SSL result status: " + result.getStatus());
            }
        }
        return decryptedBytes;
    }

    public synchronized int write(ByteBuffer src) throws IOException {
        if (pendingWrite && !completeWrite()) {
            return 0;
        }
        int encryptedBytes = 0;
        while (src.hasRemaining()) {
            // chunk src to encrypted ssl records of pocket size
            outEncryptedData.clear();
            final SSLEngineResult result = engine.wrap(src, outEncryptedData);
            switch (result.getStatus()) {
                case OK:
                    outEncryptedData.flip();
                    encryptedBytes += result.bytesConsumed();
                    while (outEncryptedData.hasRemaining()) {
                        final int written = socketChannel.write(outEncryptedData);
                        if (written == 0) {
                            pendingWrite = true;
                            return encryptedBytes;
                        }
                    }
                    break;
                case BUFFER_OVERFLOW:
                    outEncryptedData = NetworkUtil.enlargeSslPacketBuffer(engine, outEncryptedData);
                    break;
                case CLOSED:
                    close();
                    return -1;
                case BUFFER_UNDERFLOW:
                default:
                    throw new IllegalStateException("Invalid SSL result status: " + result.getStatus());
            }
        }
        pendingWrite = false;
        return encryptedBytes;
    }

    @Override
    public synchronized boolean completeWrite() throws IOException {
        while (outEncryptedData.hasRemaining()) {
            final int written = socketChannel.write(outEncryptedData);
            if (written == 0) {
                return false;
            }
        }
        pendingWrite = false;
        return true;
    }

    @Override
    public synchronized void close() throws IOException {
        if (socketChannel.isOpen()) {
            engine.closeOutbound();
            new SslHandshake(this).handshake();
            socketChannel.close();
        }
    }

    @Override
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    @Override
    public synchronized boolean isPendingRead() {
        return cachedData;
    }

    @Override
    public synchronized boolean isPendingWrite() {
        return pendingWrite;
    }

    public SSLEngine getSslEngine() {
        return engine;
    }

    @Override
    public String toString() {
        return getConnectionInfo();
    }

    private void handleReadUnderflow() {
        if (engine.getSession().getPacketBufferSize() > inEncryptedData.capacity()) {
            inEncryptedData = NetworkUtil.enlargeSslPacketBuffer(engine, inEncryptedData);
        } else {
            inEncryptedData.compact();
        }
        partialRecord = true;
    }

    private void handleEndOfStreamQuietly() {
        try {
            engine.closeInbound();
            close();
        } catch (Exception e) {
            LOGGER.warn("failed to close socket gracefully", e);
        }
    }

    private String getConnectionInfo() {
        try {
            return getSocketChannel().getLocalAddress() + " -> " + getSocketChannel().getRemoteAddress();
        } catch (IOException e) {
            LOGGER.warn("failed to get connection info", e);
            return "";
        }
    }

    private static int transferTo(ByteBuffer src, ByteBuffer dst) {
        final int maxTransfer = Math.min(dst.remaining(), src.remaining());
        if (maxTransfer > 0) {
            dst.put(src.array(), src.arrayOffset() + src.position(), maxTransfer);
            src.position(src.position() + maxTransfer);
        }
        return maxTransfer;
    }
}
