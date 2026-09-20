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
import java.io.InputStream;
import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Re-implementation of {@link RunLengthBitPackingHybridDecoder}
 */
public class ParquetRunLengthBitPackingHybridDecoder {
    private enum MODE {
        RLE,
        PACKED
    }

    private final int bitWidth;
    private final BytePacker packer;
    private InputStream in;

    private MODE mode;
    private int currentCount;
    private int currentValue;
    private int currentBufferLength;
    private int[] currentBuffer;
    private byte[] bytes;

    public ParquetRunLengthBitPackingHybridDecoder(int bitWidth) {
        Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
        this.bitWidth = bitWidth;
        this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    }

    public void reset(InputStream in) {
        this.in = in;
        currentCount = 0;
        currentBufferLength = 0;
    }

    public int readInt() throws HyracksDataException {
        try {
            return nextInt();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private int nextInt() throws IOException {
        if (currentCount == 0) {
            readNext();
        }
        --currentCount;
        int result;
        switch (mode) {
            case RLE:
                result = currentValue;
                break;
            case PACKED:
                result = currentBuffer[currentBufferLength - 1 - currentCount];
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
        return result;
    }

    /**
     * Consumes consecutive entries equal to {@code value}, at most {@code limit} of them, and returns how many were
     * consumed. The first entry that differs is left for {@link #readInt()}. A run-length run is consumed by
     * adjusting its remaining count, so the cost of skipping an array of identical levels does not depend on the
     * array's length; a bit-packed group is scanned in its decoded buffer.
     * <p>
     * The stream's last bit-packed group is padded with zeros that are indistinguishable from real entries, so the
     * caller must bound {@code limit} by the number of entries it knows remain, as it already does for
     * {@link #readInt()}.
     */
    public int skipWhile(int value, int limit) throws HyracksDataException {
        int skipped = 0;
        try {
            while (skipped < limit) {
                if (currentCount == 0) {
                    if (in.available() <= 0) {
                        // leave the exhausted stream to readInt(), which reports it the same way it always has
                        break;
                    }
                    readNext();
                }
                if (mode == MODE.RLE) {
                    if (currentValue != value) {
                        break;
                    }
                    int n = Math.min(currentCount, limit - skipped);
                    currentCount -= n;
                    skipped += n;
                } else {
                    int start = currentBufferLength - currentCount;
                    int end = start + Math.min(currentCount, limit - skipped);
                    int i = start;
                    while (i < end && currentBuffer[i] == value) {
                        i++;
                    }
                    currentCount -= i - start;
                    skipped += i - start;
                    if (i < end) {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return skipped;
    }

    /**
     * Skips {@code count} entries whatever their values, a run at a time.
     */
    public void skip(int count) throws HyracksDataException {
        try {
            while (count > 0) {
                if (currentCount == 0) {
                    readNext();
                }
                int n = Math.min(currentCount, count);
                currentCount -= n;
                count -= n;
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void readNext() throws IOException {
        Preconditions.checkArgument(in.available() > 0, "Reading past RLE/BitPacking stream.");
        final int header = BytesUtils.readUnsignedVarInt(in);
        mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
        switch (mode) {
            case RLE:
                currentCount = header >>> 1;
                currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
                break;
            case PACKED:
                int numGroups = header >>> 1;
                currentCount = numGroups * 8;
                allocateBuffers(currentCount, numGroups * bitWidth);
                // At the end of the file RLE data though, there might not be that many bytes left.
                int bytesToRead = (int) Math.ceil(currentCount * bitWidth / 8.0);
                bytesToRead = Math.min(bytesToRead, in.available());
                readFully(bytes, bytesToRead);
                for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex +=
                        bitWidth) {
                    packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
                }
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
    }

    private void allocateBuffers(int intBufferLength, int byteBufferLength) {
        if (currentBuffer == null || currentBuffer.length < intBufferLength) {
            currentBuffer = new int[intBufferLength];
        } else {
            Arrays.fill(currentBuffer, 0);
        }
        currentBufferLength = intBufferLength;

        if (bytes == null || bytes.length < byteBufferLength) {
            bytes = new byte[byteBufferLength];
        } else {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    private void readFully(byte[] b, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        int n = 0;
        while (n < len) {
            int count = in.read(b, n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }
}
