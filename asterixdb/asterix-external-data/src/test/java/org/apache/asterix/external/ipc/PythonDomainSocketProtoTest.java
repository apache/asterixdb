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
package org.apache.asterix.external.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

/**
 * Drives {@link PythonDomainSocketProto} against a hand-written peer on the other end of a real
 * UNIX domain socket, so that the responses a misbehaving UDF executor can produce - an ERROR
 * frame with no message in it, and a connection that dies halfway through a response - are
 * reproducible without a Python interpreter.
 */
public class PythonDomainSocketProtoTest {

    private Path sockDir;
    private ServerSocketChannel listener;
    private SocketChannel asterixSide;
    private SocketChannel executorSide;
    private PythonDomainSocketProto proto;

    @Before
    public void setUp() throws IOException {
        sockDir = Files.createTempDirectory("pyudf-proto-test");
        UnixDomainSocketAddress addr = UnixDomainSocketAddress.of(sockDir.resolve("pyudf.socket"));
        listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        listener.bind(addr);
        asterixSide = SocketChannel.open(addr);
        executorSide = listener.accept();
        proto = new PythonDomainSocketProto(Channels.newOutputStream(asterixSide), asterixSide, sockDir.toString());
    }

    @After
    public void tearDown() throws IOException {
        close(executorSide);
        close(asterixSide);
        close(listener);
        Files.walk(sockDir).sorted((l, r) -> r.getNameCount() - l.getNameCount()).forEach(p -> p.toFile().delete());
    }

    @Test
    public void wellFormedResponseIsDecoded() throws Exception {
        sendFrame(MessageType.HELO, packInt(4242));
        proto.receiveMsg();
        assertEquals(MessageType.HELO, proto.getResponseType());
    }

    @Test
    public void errorWithMessageIsReported() throws Exception {
        sendFrame(MessageType.ERROR, pack("ImportError: Module was not found in library"));
        assertUdfException("ImportError: Module was not found in library");
    }

    @Test
    public void nullsInErrorMessageAreScrubbed() throws Exception {
        sendFrame(MessageType.ERROR, pack("Type\0Error"));
        assertUdfException("Type Error");
    }

    @Test
    public void errorWithNoMessageIsReported() throws Exception {
        sendFrame(MessageType.ERROR, new byte[0]);
        assertUdfException("UDF executor raised an error, but with no error message.");
    }

    @Test
    public void errorWithEmptyMessageIsReported() throws Exception {
        sendFrame(MessageType.ERROR, pack(""));
        assertUdfException("UDF executor raised an error, but with no error message.");
    }

    @Test
    public void errorWithNilMessageIsReported() throws Exception {
        sendFrame(MessageType.ERROR, packNil());
        assertUdfException("UDF executor raised an error, but with no error message.");
    }

    @Test
    public void closedConnectionIsReported() throws Exception {
        executorSide.close();
        assertUdfException("UDF Executor ended the stream unexpectedly while sending output.");
    }

    @Test
    public void truncatedHeaderIsReported() throws Exception {
        write(ByteBuffer.allocate(PythonDomainSocketProto.HYR_HEADER_SIZE - 1));
        executorSide.close();
        assertUdfException("UDF Executor ended the stream unexpectedly while sending output.");
    }

    @Test
    public void truncatedBodyIsReported() throws Exception {
        byte[] body = pack("ArithmeticError: oof");
        ByteBuffer frame = frame(MessageType.ERROR, body);
        frame.limit(frame.limit() - body.length / 2);
        write(frame);
        executorSide.close();
        assertUdfException("UDF Executor ended the stream unexpectedly while sending output.");
    }

    private void assertUdfException(String expectedMessage) throws Exception {
        try {
            proto.receiveMsg();
            fail("expected an AsterixException");
        } catch (AsterixException e) {
            assertEquals(ErrorCode.EXTERNAL_UDF_EXCEPTION.intValue(), e.getErrorCode());
            assertEquals(expectedMessage, e.getParams()[0]);
            assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }

    private static byte[] pack(String s) throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packString(s);
        return packer.toByteArray();
    }

    private static byte[] packInt(int value) throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packInt(value);
        return packer.toByteArray();
    }

    private static byte[] packNil() throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packNil();
        return packer.toByteArray();
    }

    private static ByteBuffer frame(MessageType type, byte[] body) {
        int dataLength = body.length + 1;
        ByteBuffer frame = ByteBuffer.allocate(PythonDomainSocketProto.HYR_HEADER_SIZE + dataLength);
        frame.putInt(dataLength + PythonDomainSocketProto.HYR_HEADER_SIZE_NOSZ);
        frame.putLong(-1L);
        frame.putLong(0L);
        frame.put((byte) 0);
        frame.put((byte) type.ordinal());
        frame.put(body);
        frame.flip();
        return frame;
    }

    private void sendFrame(MessageType type, byte[] body) throws IOException {
        write(frame(type, body));
    }

    private void write(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            executorSide.write(buf);
        }
    }

    private static void close(Channel channel) throws IOException {
        if (channel != null) {
            channel.close();
        }
    }
}
