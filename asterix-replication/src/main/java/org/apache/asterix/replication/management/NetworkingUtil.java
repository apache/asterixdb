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

public class NetworkingUtil {

    public static void readBytes(SocketChannel socketChannel, ByteBuffer byteBuffer, int length) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(length);

        while (byteBuffer.remaining() > 0 && socketChannel.read(byteBuffer) > 0);

        if (byteBuffer.remaining() > 0) {
            throw new EOFException();
        }

        byteBuffer.flip();
    }

    public static void sendFile(FileChannel fileChannel, SocketChannel socketChannel) throws IOException {
        long pos = 0;
        long fileSize = fileChannel.size();
        long remainingBytes = fileSize;
        long transferredBytes = 0;

        while ((transferredBytes += fileChannel.transferTo(pos, remainingBytes, socketChannel)) < fileSize) {
            pos += transferredBytes;
            remainingBytes -= transferredBytes;
        }

        socketChannel.socket().getOutputStream().flush();
    }

    public static void downloadFile(FileChannel fileChannel, SocketChannel socketChannel) throws IOException {
        long pos = 0;
        long fileSize = fileChannel.size();
        long count = fileSize;
        long numTransferred = 0;
        while ((numTransferred += fileChannel.transferFrom(socketChannel, pos, count)) < fileSize) {
            pos += numTransferred;
            count -= numTransferred;
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

    public static void transferBufferToChannel(SocketChannel socketChannel, ByteBuffer requestBuffer) throws IOException {
        while (requestBuffer.hasRemaining()) {
            socketChannel.write(requestBuffer);
        }
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
}