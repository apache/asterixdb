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
package org.apache.hyracks.api.network;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

public interface ISocketChannel extends WritableByteChannel, ReadableByteChannel, Closeable {

    /**
     * Indicates whether this {@link ISocketChannel} requires a client/server handshake before
     * exchanging application data
     *
     * @return true if the socket requires handshake, otherwise false.
     */
    boolean requiresHandshake();

    /**
     * Performs the handshake operations.
     *
     * @return true, if the handshake is successful. Otherwise false.
     */
    boolean handshake();

    /**
     * Indicates if this {@link ISocketChannel} has data that is ready for reading.
     *
     * @return true, if the socket has data ready for reading. Otherwise false.
     */
    boolean isPendingRead();

    /**
     * Attempts to read data into {@code dst} buffer. The position of the byte buffer
     * is incremented by the number of read bytes.
     *
     * @param dst
     * @return The number of bytes transferred into the buffer.
     * @throws IOException
     */
    int read(ByteBuffer dst) throws IOException;

    /**
     * Attempts to write data from the {@code src} buffer. The position of the byte buffer
     * is incremented by the number of written bytes. A write operation may not fully write
     * the number of consumed bytes from the {@code src} buffer. The caller may check if any data
     * is still pending writing using {@link ISocketChannel#isPendingWrite()}. An attempt can be
     * made to complete the write operation using {@link ISocketChannel#completeWrite()}
     *
     * @param src
     * @return The number of bytes consumed from the buffer.
     * @throws IOException
     */
    int write(ByteBuffer src) throws IOException;

    /**
     * Indicates if this {@link ISocketChannel} has data pending write completion.
     *
     * @return true, if the socket has data pending write. Otherwise false.
     */
    boolean isPendingWrite();

    /**
     * Attempts to write any data pending write.
     *
     * @return true, if all data pending write has been written. Otherwise false.
     * @throws IOException
     */
    boolean completeWrite() throws IOException;

    /**
     * Gets the network socket channel behind this {@link ISocketChannel}
     *
     * @return the socket channel
     */
    SocketChannel getSocketChannel();

    @Override
    default boolean isOpen() {
        return getSocketChannel().isOpen();
    }
}