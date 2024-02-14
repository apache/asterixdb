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
package org.apache.asterix.replication.management;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.Enumeration;

import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetworkingUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private NetworkingUtil() {
        throw new AssertionError("This util class should not be initialized.");
    }

    public static void readBytes(ISocketChannel socketChannel, ByteBuffer byteBuffer, int length) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(length);

        while (byteBuffer.remaining() > 0 && socketChannel.read(byteBuffer) >= 0);

        if (byteBuffer.remaining() > 0) {
            throw new EOFException("could not read all data from source; remaining bytes: " + byteBuffer.remaining());
        }

        byteBuffer.flip();
    }

    public static void sendFile(FileChannel fileChannel, ISocketChannel socketChannel) throws IOException {
        long pos = 0;
        long remainingBytes = fileChannel.size();
        try {
            while (remainingBytes > 0) {
                long sentBytes = fileChannel.transferTo(pos, remainingBytes, socketChannel);
                pos += sentBytes;
                remainingBytes -= sentBytes;
            }
            socketChannel.getSocketChannel().socket().getOutputStream().flush();
        } catch (Exception e) {
            LOGGER.info("failed to send file; file size {}, pos {}, remainingBytes {}", fileChannel.size(), pos,
                    remainingBytes);
        }
    }

    public static void downloadFile(FileChannel fileChannel, ISocketChannel socketChannel) throws IOException {
        long remainingBytes = fileChannel.size();
        long pos = 0;
        try {
            while (remainingBytes > 0) {
                long readBytes = fileChannel.transferFrom(socketChannel, pos, remainingBytes);
                pos += readBytes;
                remainingBytes -= readBytes;
            }
        } catch (Exception e) {
            LOGGER.info("failed to download file; file size {}, pos {}, remainingBytes {}", fileChannel.size(), pos,
                    remainingBytes);
            throw e;
        }
    }

    public static String getHostAddress(String hostIPAddressFirstOctet) throws SocketException {
        String hostName = null;
        Enumeration<NetworkInterface> nInterfaces = NetworkInterface.getNetworkInterfaces();
        while (nInterfaces.hasMoreElements()) {
            if (hostName != null) {
                break;
            }
            Enumeration<InetAddress> inetAddresses = nInterfaces.nextElement().getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                String address = inetAddresses.nextElement().getHostAddress();
                if (address.startsWith(hostIPAddressFirstOctet)) {
                    hostName = address;
                    break;
                }
            }
        }
        return hostName;
    }

    public static void transferBufferToChannel(ISocketChannel socketChannel, ByteBuffer requestBuffer)
            throws IOException {
        while (requestBuffer.hasRemaining()) {
            socketChannel.write(requestBuffer);
        }
        socketChannel.getSocketChannel().socket().getOutputStream().flush();
    }

    //unused
    public static void sendFileNIO(FileChannel fileChannel, SocketChannel socketChannel) throws IOException {
        long fileSize = fileChannel.size();
        MappedByteBuffer bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        socketChannel.write(bb);
    }

    //unused
    public static void downloadFileNIO(FileChannel fileChannel, SocketChannel socketChannel) throws IOException {
        long pos = 0;
        long fileSize = fileChannel.size();
        fileChannel.transferFrom(socketChannel, pos, fileSize);
    }

    public static boolean isHealthy(ISocketChannel sc) {
        return sc != null && sc.getSocketChannel().isOpen() && sc.getSocketChannel().isConnected();
    }
}
