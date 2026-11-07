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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.apache.hyracks.util.NetworkUtil;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SslHandshake {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final String HANDSHAKE_TIMEOUT_SEC_KEY = "hyracks.ssl.handshake.timeout.sec";
    // TODO(mblow): this could be a config parameter
    private static final long HANDSHAKE_TIMEOUT_NANOS =
            TimeUnit.SECONDS.toNanos(Integer.getInteger(HANDSHAKE_TIMEOUT_SEC_KEY, 60));

    private final ByteBuffer handshakeOutData;
    private final SocketChannel socketChannel;
    private final SSLEngine engine;
    private SSLEngineResult.HandshakeStatus handshakeStatus;
    private ByteBuffer handshakeInData;
    private ByteBuffer outEncryptedData;
    private ByteBuffer inEncryptedData;
    private Selector selector;
    private long deadlineNanos;

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

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "handshake deadline, inline tasks")
    public boolean handshake() throws IOException {
        deadlineNanos = System.nanoTime() + HANDSHAKE_TIMEOUT_NANOS;
        try {
            handshakeStatus = engine.getHandshakeStatus();
            while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
                    && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                if (System.nanoTime() - deadlineNanos >= 0) {
                    LOGGER.warn("SSL handshake with {} timed out in state {}", socketChannel, handshakeStatus);
                    return false;
                }
                switch (handshakeStatus) {
                    case NEED_UNWRAP:
                        if (!unwrap()) {
                            return false;
                        }
                        break;
                    case NEED_WRAP:
                        if (!wrap()) {
                            return false;
                        }
                        break;
                    case NEED_TASK:
                        Runnable task;
                        // run inline; the handshake already owns a thread of its own
                        while ((task = engine.getDelegatedTask()) != null) {
                            task.run();
                        }
                        handshakeStatus = engine.getHandshakeStatus();
                        break;
                    default:
                        throw new IllegalStateException("Invalid SSL handshake status: " + handshakeStatus);
                }
            }
            return true;
        } finally {
            NetworkUtil.closeQuietly(selector);
        }
    }

    private boolean awaitReadable() throws IOException {
        return await(SelectionKey.OP_READ, "data");
    }

    private boolean awaitWritable() throws IOException {
        return await(SelectionKey.OP_WRITE, "buffer space");
    }

    /**
     * Blocks until the channel is ready for the given operation, the handshake deadline expires, or the
     * wait is interrupted. The channel is non-blocking, so without this a read or write making no progress
     * would be retried immediately, spinning a core until the peer acts.
     *
     * @return true if the operation should be retried, false if the handshake deadline has passed
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "await readability within deadline")
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "generalized to await writability")
    private boolean await(int op, String what) throws IOException {
        final long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            LOGGER.warn("SSL handshake with {} timed out awaiting {}", socketChannel, what);
            return false;
        }
        if (selector == null) {
            selector = Selector.open();
        }
        // re-registering against the same selector just updates the interest set
        socketChannel.register(selector, op);
        // select() may return spuriously; the caller re-checks the deadline on the next pass
        selector.select(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        selector.selectedKeys().clear();
        if (Thread.currentThread().isInterrupted()) {
            LOGGER.warn("SSL handshake with {} interrupted awaiting {}", socketChannel, what);
            return false;
        }
        return true;
    }

    /**
     * Writes out the encrypted handshake data, waiting for the peer within the handshake deadline whenever
     * the channel accepts no bytes.
     *
     * @return true if the buffer was fully written, false if the handshake deadline has passed
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "bounded flush of handshake data")
    private boolean flush() throws IOException {
        while (outEncryptedData.hasRemaining()) {
            if (socketChannel.write(outEncryptedData) == 0 && !awaitWritable()) {
                return false;
            }
        }
        return true;
    }

    private boolean wrap() throws IOException {
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
                return flush();
            case BUFFER_OVERFLOW:
                outEncryptedData = NetworkUtil.enlargeSslPacketBuffer(engine, outEncryptedData);
                return true;
            case CLOSED:
                outEncryptedData.flip();
                if (!flush()) {
                    return false;
                }
                inEncryptedData.clear();
                handshakeStatus = engine.getHandshakeStatus();
                return true;
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
        if (read == 0 && inEncryptedData.position() == 0) {
            // nothing buffered and nothing arrived; wait for the peer rather than spinning on the channel
            return awaitReadable();
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
                // only a partial record is buffered; more bytes are needed before a retry can make progress
                return awaitReadable();
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
