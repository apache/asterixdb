/*
 * Copyright 2009-2011 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.api.aqlj.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

/**
 * This class provides a clean mechanism for sending and receiving the data
 * types involved in the AQLJ protocol.
 * 
 * @author zheilbron
 */
public class AQLJStream {
    private static final int BUF_SIZE = 8192;

    private final String host;
    private final int port;
    private final Socket connection;
    private final BufferedInputStream aqljInput;
    private final BufferedOutputStream aqljOutput;

    private final byte[] int16Buf;
    private final byte[] int32Buf;

    public AQLJStream(String host, int port) throws IOException {
        this.host = host;
        this.port = port;

        connection = new Socket(host, port);

        aqljInput = new BufferedInputStream(connection.getInputStream(), BUF_SIZE);
        aqljOutput = new BufferedOutputStream(connection.getOutputStream(), BUF_SIZE);

        int16Buf = new byte[2];
        int32Buf = new byte[4];
    }

    public AQLJStream(Socket sock) throws IOException {
        this.host = null;
        this.port = 0;

        this.connection = sock;
        aqljInput = new BufferedInputStream(connection.getInputStream(), BUF_SIZE);
        aqljOutput = new BufferedOutputStream(connection.getOutputStream(), BUF_SIZE);

        int16Buf = new byte[2];
        int32Buf = new byte[4];
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Socket getSocket() {
        return connection;
    }

    public void receive(byte[] buf, int off, int len) throws IOException {
        int read;
        int count = 0;
        while (count < len) {
            read = aqljInput.read(buf, off + count, len - count);
            if (read < 0) {
                throw new EOFException();
            }
            count += read;
        }
    }

    public byte[] receive(int len) throws IOException {
        byte[] result = new byte[len];
        receive(result, 0, len);
        return result;
    }

    public int receiveInt16() throws IOException {
        if (aqljInput.read(int16Buf) != 2) {
            throw new EOFException();
        }
        return (int16Buf[0] & 0xff) << 8 | (int16Buf[1] & 0xff);
    }

    public long receiveUnsignedInt32() throws IOException {
        if (aqljInput.read(int32Buf) != 4) {
            throw new EOFException();
        }
        return ((int32Buf[0] & 0xff) << 24 | (int32Buf[1] & 0xff) << 16 | (int32Buf[2] & 0xff) << 8 | (int32Buf[3] & 0xff)) & 0x00000000ffffffffl;
    }

    public int receiveChar() throws IOException {
        int c = aqljInput.read();
        if (c < 0) {
            throw new EOFException();
        }
        return c;
    }

    public String receiveString() throws IOException {
        int strlen = receiveInt16();
        return new String(receive(strlen), "UTF8");
    }

    public void send(byte[] buf) throws IOException {
        aqljOutput.write(buf);
    }

    public void send(byte[] buf, int off, int len) throws IOException {
        aqljOutput.write(buf, off, len);
    }

    public void sendInt16(int val) throws IOException {
        int16Buf[0] = (byte) (val >>> 8);
        int16Buf[1] = (byte) (val);
        aqljOutput.write(int16Buf);
    }

    public void sendUnsignedInt32(long val) throws IOException {
        int32Buf[0] = (byte) (val >>> 24);
        int32Buf[1] = (byte) (val >>> 16);
        int32Buf[2] = (byte) (val >>> 8);
        int32Buf[3] = (byte) (val);
        aqljOutput.write(int32Buf);
    }

    public void sendChar(int c) throws IOException {
        aqljOutput.write(c);
    }

    public void sendString(byte[] strBytes) throws IOException {
        sendInt16(strBytes.length);
        send(strBytes);
    }

    public void sendString(String str) throws IOException {
        byte[] strBytes = str.getBytes("UTF8");
        sendInt16(strBytes.length);
        send(strBytes);
    }

    public void flush() throws IOException {
        aqljOutput.flush();
    }

    public void close() throws IOException {
        aqljInput.close();
        aqljOutput.close();
        connection.close();
    }
}
