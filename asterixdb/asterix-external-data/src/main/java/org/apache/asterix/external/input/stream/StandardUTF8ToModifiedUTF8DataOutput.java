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
package org.apache.asterix.external.input.stream;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;

/**
 * Writes modified UTF-8 string format to {@link StandardUTF8ToModifiedUTF8DataOutput#out}
 * from standard UTF-8 string format.
 */
public class StandardUTF8ToModifiedUTF8DataOutput implements DataOutput {
    private static final byte[] EMPTY = new byte[0];
    private final AStringSerializerDeserializer stringSerDer;
    private final ResettableUTF8InputStreamReader reader;
    private final char[] inputBuffer;
    private char[] appendBuffer;
    private DataOutput out;

    public StandardUTF8ToModifiedUTF8DataOutput(AStringSerializerDeserializer stringSerDer) {
        this.stringSerDer = stringSerDer;
        reader = new ResettableUTF8InputStreamReader(new ByteArrayAccessibleInputStream(EMPTY, 0, 0));
        inputBuffer = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
        appendBuffer = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
    }

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        reader.prepareNextRead(b, off, len);
        int numOfChars = reader.read(inputBuffer);
        int length = 0;
        while (numOfChars > 0) {
            appendBuffer = append(inputBuffer, appendBuffer, length, numOfChars);
            length += numOfChars;
            numOfChars = reader.read(inputBuffer);
        }
        stringSerDer.serialize(appendBuffer, 0, length, out);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeByte(int v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeShort(int v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeChar(int v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeInt(int v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLong(long v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeFloat(float v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(double v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void setDataOutput(DataOutput out) {
        this.out = out;
    }

    private static char[] append(char[] src, char[] dest, int offset, int length) {
        char[] destBuf = dest;
        if (offset + length > dest.length) {
            char[] newDestBuffer = new char[dest.length * 2];
            System.arraycopy(destBuf, 0, newDestBuffer, 0, offset);
            destBuf = newDestBuffer;
        }
        System.arraycopy(src, 0, destBuf, offset, length);
        return destBuf;
    }

    private static class ResettableUTF8InputStreamReader extends AsterixInputStreamReader {
        private final ByteArrayAccessibleInputStream inByte;

        public ResettableUTF8InputStreamReader(ByteArrayAccessibleInputStream inByte) {
            super(new BasicInputStream(inByte));
            this.inByte = inByte;
        }

        //Rewind the reader after setting the byte array
        public void prepareNextRead(byte[] b, int off, int len) {
            inByte.setContent(b, off, len);
            done = false;
            remaining = false;
            byteBuffer.flip();
            charBuffer.flip();
        }

    }
}
