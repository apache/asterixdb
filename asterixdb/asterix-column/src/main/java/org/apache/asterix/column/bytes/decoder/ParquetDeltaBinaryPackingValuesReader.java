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
package org.apache.asterix.column.bytes.decoder;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.asterix.column.bytes.ParquetDeltaBinaryPackingConfig;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Re-implementation of {@link DeltaBinaryPackingValuesReader}
 */
public class ParquetDeltaBinaryPackingValuesReader extends AbstractParquetValuesReader {
    private int totalValueCount;
    /**
     * values read by the caller
     */
    private int valuesBufferedRead;
    private int valuesRead;

    /**
     * stores the decoded values including the first value which is written to the header
     */
    private long[] valuesBuffer;
    /**
     * values loaded to the buffer, it could be bigger than the totalValueCount
     * when data is not aligned to mini block, which means padding 0s are in the buffer
     */
    private int valuesBuffered;
    private AbstractBytesInputStream in;
    private ParquetDeltaBinaryPackingConfig config;
    private int[] bitWidths;
    private ByteBuffer bitWidthBuffer;
    private long lastElement;

    /**
     * Loads one block at a time instead of eagerly loading all blocks in {@link DeltaBinaryPackingValuesReader}.
     * This is to fix the {@link #valuesBuffer} size
     */
    @Override
    public void initFromPage(AbstractBytesInputStream stream) throws IOException {
        this.in = stream;
        this.config = ParquetDeltaBinaryPackingConfig.readConfig(in, this.config);
        this.totalValueCount = BytesUtils.readUnsignedVarInt(in);
        allocateValuesBuffer();
        bitWidths = allocate(bitWidths, config.getMiniBlockNumInABlock());
        valuesBuffered = 0;

        valuesBufferedRead = 0;
        valuesRead = 0;

        //read first value from header
        valuesBuffer[valuesBuffered++] = BytesUtils.readZigZagVarLong(in);
        lastElement = valuesBuffer[0];

        if (valuesBuffered < totalValueCount) {
            loadNewBlockToBuffer();
        }
    }

    /**
     * the value buffer is allocated so that the size of it is multiple of mini block
     * because when writing, data is flushed on a mini block basis
     */
    private void allocateValuesBuffer() {
        //+ 1 because first value written to header is also stored in values buffer
        final int bufferSize = config.getMiniBlockSizeInValues() * config.getMiniBlockNumInABlock() + 1;
        if (valuesBuffer == null || valuesBuffer.length < bufferSize) {
            valuesBuffer = new long[bufferSize];
        } else {
            Arrays.fill(valuesBuffer, 0);
        }
    }

    private int[] allocate(int[] array, int size) {
        if (array == null || array.length < size) {
            return new int[size];
        }
        return array;
    }

    @Override
    public void skip() {
        checkRead();
        valuesRead++;
    }

    @Override
    public int readInteger() {
        // TODO: probably implement it separately
        return (int) readLong();
    }

    @Override
    public long readLong() {
        checkRead();
        valuesRead++;
        return valuesBuffer[valuesBufferedRead++];
    }

    private void checkRead() {
        if (valuesRead >= totalValueCount) {
            throw new ParquetDecodingException("no more value to read, total value count is " + totalValueCount);
        }
        if (valuesBufferedRead >= valuesBuffered) {
            //Set the last value buffered as the first
            lastElement = valuesBuffer[valuesBufferedRead - 1];
            valuesBufferedRead = 0;
            valuesBuffered = 0;
            Arrays.fill(valuesBuffer, 0);
            try {
                loadNewBlockToBuffer();
            } catch (IOException e) {
                throw new ParquetDecodingException("can not load next block", e);
            }

        }
    }

    private void loadNewBlockToBuffer() throws IOException {
        long minDeltaInCurrentBlock;
        try {
            minDeltaInCurrentBlock = BytesUtils.readZigZagVarLong(in);
        } catch (IOException e) {
            throw new ParquetDecodingException("can not read min delta in current block", e);
        }

        readBitWidthsForMiniBlocks();

        // mini block is atomic for reading, we read a mini block when there are more values left
        int i;
        for (i = 0; i < config.getMiniBlockNumInABlock() && valuesRead + valuesBuffered < totalValueCount; i++) {
            BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidths[i]);
            unpackMiniBlock(packer);
        }

        //calculate values from deltas unpacked for current block
        int valueUnpacked = i * config.getMiniBlockSizeInValues();
        long prev = lastElement;
        for (int j = valuesBuffered - valueUnpacked; j < valuesBuffered; j++) {
            valuesBuffer[j] += minDeltaInCurrentBlock + prev;
            prev = valuesBuffer[j];
        }
    }

    /**
     * mini block has a size of 8*n, unpack 8 value each time
     *
     * @param packer the packer created from bitwidth of current mini block
     */
    private void unpackMiniBlock(BytePackerForLong packer) throws IOException {
        for (int j = 0; j < config.getMiniBlockSizeInValues(); j += 8) {
            unpack8Values(packer);
        }
    }

    private void unpack8Values(BytePackerForLong packer) throws IOException {
        // get a single buffer of 8 values. most of the time, this won't require a copy
        ByteBuffer buffer = readBitWidth(packer.getBitWidth());
        packer.unpack8Values(buffer, buffer.position(), valuesBuffer, valuesBuffered);
        this.valuesBuffered += 8;
    }

    private void readBitWidthsForMiniBlocks() {
        for (int i = 0; i < config.getMiniBlockNumInABlock(); i++) {
            try {
                bitWidths[i] = BytesUtils.readIntLittleEndianOnOneByte(in);
            } catch (IOException e) {
                throw new ParquetDecodingException("Can not decode bit width in block header", e);
            }
        }
    }

    private ByteBuffer prepareBitWidthBuffer(int length) {
        if (bitWidthBuffer == null || bitWidthBuffer.capacity() < length) {
            bitWidthBuffer = ByteBuffer.allocate(length);
        }
        bitWidthBuffer.clear();
        bitWidthBuffer.limit(length);
        return bitWidthBuffer;
    }

    private ByteBuffer readBitWidth(int length) throws IOException {
        ByteBuffer buffer = prepareBitWidthBuffer(length);
        int read = in.read(buffer);
        if (read != length) {
            throw new EOFException("Reached end of stream");
        }
        buffer.position(0);
        return buffer;
    }
}
