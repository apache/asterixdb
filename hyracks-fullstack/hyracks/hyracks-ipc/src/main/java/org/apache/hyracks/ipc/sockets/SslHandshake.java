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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.apache.hyracks.util.NetworkUtil;

public class SslHandshake {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ByteBuffer handshakeOutData;
    private final SocketChannel socketChannel;
    private final SSLEngine engine;
    private SSLEngineResult.HandshakeStatus handshakeStatus;
    private ByteBuffer handshakeInData;
    private ByteBuffer outEncryptedData;
    private ByteBuffer inEncryptedData;

    public SslHandshake(SslSocketChannel sslSocketChannel) {
        socketChannel = sslSocketChannel.getSocketChannel();
        engine = sslSocketChannel.getSslEngine();
        final int pocketBufferSize = engine.getSession().getPacketBufferSize();
        inEncryptedData = ByteBuffer.allocate(pocketBufferSize);
        outEncryptedData = ByteBuffer.allocate(pocketBufferSize);
        // increase app buffer size to reduce possibility of overflow
        final int appBufferSize = engine.getSession().getApplicationBufferSize() + 50;
        handshakeOutData = ByteBuffer.allocate(appBufferSize);
        handshakeInData = ByteBuffer.allocate(appBufferSize);
    }

    public boolean handshake() throws IOException {
        handshakeStatus = engine.getHandshakeStatus();
        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
                && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    if (!unwrap()) {
                        return false;
                    }
                    break;
                case NEED_WRAP:
                    wrap();
                    break;
                case NEED_TASK:
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) {
                        executor.execute(task);
                    }
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                default:
                    throw new IllegalStateException("Invalid SSL handshake status: " + handshakeStatus);
            }
        }
        return true;
    }

    private void wrap() throws IOException {
        outEncryptedData.clear();
        SSLEngineResult result;
        try {
            result = engine.wrap(handshakeOutData, outEncryptedData);
            handshakeStatus = result.getHandshakeStatus();
        } catch (SSLException sslException) {
            engine.closeOutbound();
            handshakeStatus = engine.getHandshakeStatus();
            throw sslException;
        }
        switch (result.getStatus()) {
            case OK:
                outEncryptedData.flip();
                while (outEncryptedData.hasRemaining()) {
                    socketChannel.write(outEncryptedData);
                }
                break;
            case BUFFER_OVERFLOW:
                outEncryptedData = NetworkUtil.enlargeSslPacketBuffer(engine, outEncryptedData);
                break;
            case CLOSED:
                outEncryptedData.flip();
                while (outEncryptedData.hasRemaining()) {
                    socketChannel.write(outEncryptedData);
                }
                inEncryptedData.clear();
                handshakeStatus = engine.getHandshakeStatus();
                break;
            case BUFFER_UNDERFLOW:
            default:
                throw new IllegalStateException("Invalid SSL status " + result.getStatus());
        }
    }

    private boolean unwrap() throws IOException {
        final int read = socketChannel.read(inEncryptedData);
        if (read < 0) {
            if (engine.isInboundDone() && engine.isOutboundDone()) {
                return false;
            }
            engine.closeInbound();
            // close output to put engine in WRAP status to attempt graceful ssl session end
            engine.closeOutbound();
            return false;
        }
        inEncryptedData.flip();
        SSLEngineResult result;
        try {
            result = engine.unwrap(inEncryptedData, handshakeInData);
            inEncryptedData.compact();
            handshakeStatus = result.getHandshakeStatus();
        } catch (SSLException sslException) {
            engine.closeOutbound();
            handshakeStatus = engine.getHandshakeStatus();
            throw sslException;
        }
        switch (result.getStatus()) {
            case OK:
                break;
            case BUFFER_OVERFLOW:
                handshakeInData = NetworkUtil.enlargeSslApplicationBuffer(engine, handshakeInData);
                break;
            case BUFFER_UNDERFLOW:
                inEncryptedData = handleBufferUnderflow(engine, inEncryptedData);
                break;
            case CLOSED:
                if (engine.isOutboundDone()) {
                    return false;
                } else {
                    engine.closeOutbound();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
            default:
                throw new IllegalStateException("Invalid SSL status " + result.getStatus());
        }
        return true;
    }

    private ByteBuffer handleBufferUnderflow(SSLEngine engine, ByteBuffer buffer) {
        if (buffer.capacity() >= engine.getSession().getPacketBufferSize()) {
            return buffer;
        } else {
            final ByteBuffer replaceBuffer = NetworkUtil.enlargeSslPacketBuffer(engine, buffer);
            buffer.flip();
            replaceBuffer.put(buffer);
            return replaceBuffer;
        }
    }
}
