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
package org.apache.asterix.column.bytes.encoder;

import java.io.IOException;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.io.ParquetEncodingException;

/**
 * Re-implementation of {@link DeltaBinaryPackingValuesWriterForLong}
 */
public class ParquetDeltaBinaryPackingValuesWriterForLong extends AbstractParquetDeltaBinaryPackingValuesWriter {
    /**
     * max bitwidth for a mini block, it is used to allocate miniBlockByteBuffer which is
     * reused between flushes.
     */
    private static final int MAX_BITWIDTH = 64;

    private final int blockSizeInValues;
    private final int miniBlockNumInABlock;
    private final int miniBlockSizeInValues;

    /**
     * stores delta values starting from the 2nd value written(1st value is stored in header).
     * It's reused between flushes
     */
    private final long[] deltaBlockBuffer;

    /**
     * firstValue is written to the header of the page
     */
    private long firstValue = 0;

    /**
     * cache previous written value for calculating delta
     */
    private long previousValue = 0;

    /**
     * min delta is written to the beginning of each block.
     * it's zig-zag encoded. The deltas stored in each block is actually the difference to min delta,
     * therefore are all positive
     * it will be reset after each flush
     */
    private long minDeltaInCurrentBlock = Long.MAX_VALUE;
    private long maxDeltaInCurrentBlock = Long.MIN_VALUE;

    public ParquetDeltaBinaryPackingValuesWriterForLong(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        this(DEFAULT_NUM_BLOCK_VALUES, DEFAULT_NUM_MINIBLOCKS, multiPageOpRef);
    }

    public ParquetDeltaBinaryPackingValuesWriterForLong(int blockSizeInValues, int miniBlockNum,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        super(blockSizeInValues, miniBlockNum, multiPageOpRef);
        this.blockSizeInValues = blockSizeInValues;
        this.miniBlockNumInABlock = miniBlockNum;
        double miniSize = (double) blockSizeInValues / miniBlockNumInABlock;
        Preconditions.checkArgument(miniSize % 8 == 0, "miniBlockSize must be multiple of 8, but it's " + miniSize);
        this.miniBlockSizeInValues = (int) miniSize;
        deltaBlockBuffer = new long[blockSizeInValues];
        miniBlockByteBuffer = new byte[miniBlockSizeInValues * MAX_BITWIDTH];
    }

    @Override
    public void writeLong(long v) {
        totalValueCount++;

        if (totalValueCount == 1) {
            firstValue = v;
            previousValue = firstValue;
            return;
        }

        // Calculate delta. The possible overflow is accounted for. The algorithm is correct because
        // Java long is working as a modalar ring with base 2^64 and because of the plus and minus
        // properties of a ring. http://en.wikipedia.org/wiki/Modular_arithmetic#Integers_modulo_n
        long delta = v - previousValue;
        previousValue = v;

        deltaBlockBuffer[deltaValuesToFlush++] = delta;

        if (delta < minDeltaInCurrentBlock) {
            minDeltaInCurrentBlock = delta;
        }

        if (blockSizeInValues == deltaValuesToFlush) {
            flushBlockBuffer();
        } else {
            //Recalibrate the estimated size
            if (delta > maxDeltaInCurrentBlock) {
                maxDeltaInCurrentBlock = delta;
                estimatedElementSize =
                        (64 - Long.numberOfLeadingZeros(maxDeltaInCurrentBlock - minDeltaInCurrentBlock));
                estimatedSize = estimatedElementSize * deltaValuesToFlush;
            } else {
                estimatedSize += estimatedElementSize;
            }
        }
    }

    private void flushBlockBuffer() {
        // since we store the min delta, the deltas will be converted to be the difference to min delta
        // and all positive
        for (int i = 0; i < deltaValuesToFlush; i++) {
            deltaBlockBuffer[i] = deltaBlockBuffer[i] - minDeltaInCurrentBlock;
        }

        writeMinDelta();
        int miniBlocksToFlush = getMiniBlockCountToFlush(deltaValuesToFlush);

        calculateBitWidthsForDeltaBlockBuffer(miniBlocksToFlush);
        int minBitWidth = Integer.MAX_VALUE;
        for (int i = 0; i < miniBlockNumInABlock; i++) {
            writeBitWidthForMiniBlock(i);
            minBitWidth = Math.min(bitWidths[i], minBitWidth);
        }

        for (int i = 0; i < miniBlocksToFlush; i++) {
            // writing i th miniblock
            int currentBitWidth = bitWidths[i];
            int blockOffset = 0;
            // TODO: should this cache the packer?
            BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(currentBitWidth);
            int miniBlockStart = i * miniBlockSizeInValues;
            // pack values into the miniblock buffer, 8 at a time to get exactly currentBitWidth bytes
            for (int j = miniBlockStart; j < (i + 1) * miniBlockSizeInValues; j += 8) {
                // mini block is atomic in terms of flushing
                // This may write more values when reach to the end of data writing to last mini block,
                // since it may not be aligned to miniblock,
                // but doesn't matter. The reader uses total count to see if reached the end.
                packer.pack8Values(deltaBlockBuffer, j, miniBlockByteBuffer, blockOffset);
                blockOffset += currentBitWidth;
            }
            try {
                outputStream.write(miniBlockByteBuffer, 0, blockOffset);
            } catch (IOException e) {
                throw new ParquetEncodingException(e);
            }
        }

        minDeltaInCurrentBlock = Long.MAX_VALUE;
        maxDeltaInCurrentBlock = Long.MIN_VALUE;
        deltaValuesToFlush = 0;
        estimatedElementSize = 0;
        estimatedSize = 0;
    }

    private void writeMinDelta() {
        try {
            BytesUtils.writeZigZagVarLong(minDeltaInCurrentBlock, outputStream);
        } catch (IOException e) {
            throw new ParquetEncodingException("can not write min delta for block", e);
        }
    }

    /**
     * iterate through values in each mini block and calculate the bitWidths of max values.
     *
     * @param miniBlocksToFlush number of miniblocks
     */
    private void calculateBitWidthsForDeltaBlockBuffer(int miniBlocksToFlush) {
        for (int miniBlockIndex = 0; miniBlockIndex < miniBlocksToFlush; miniBlockIndex++) {
            long mask = 0;
            int miniStart = miniBlockIndex * miniBlockSizeInValues;

            //The end of current mini block could be the end of current block(deltaValuesToFlush) buffer
            //when data is not aligned to mini block
            int miniEnd = Math.min((miniBlockIndex + 1) * miniBlockSizeInValues, deltaValuesToFlush);

            for (int i = miniStart; i < miniEnd; i++) {
                mask |= deltaBlockBuffer[i];
            }
            bitWidths[miniBlockIndex] = 64 - Long.numberOfLeadingZeros(mask);
        }
    }

    /**
     * getBytes will trigger flushing block buffer, DO NOT write after getBytes() is called without calling reset()
     *
     * @return a BytesInput that contains the encoded page data
     */
    @Override
    public BytesInput getBytes() {
        // The Page Header should include: blockSizeInValues, numberOfMiniBlocks, totalValueCount
        if (deltaValuesToFlush != 0) {
            flushBlockBuffer();
        }
        BytesInput configBytes = BytesInput.concat(BytesInput.fromUnsignedVarInt(blockSizeInValues),
                BytesInput.fromUnsignedVarInt(miniBlockNumInABlock));
        return BytesInput.concat(configBytes, BytesInput.fromUnsignedVarInt(totalValueCount),
                BytesInput.fromZigZagVarLong(firstValue), outputStream.asBytesInput());
    }

    @Override
    public void reset() throws HyracksDataException {
        super.reset();
        this.minDeltaInCurrentBlock = Long.MAX_VALUE;
        this.maxDeltaInCurrentBlock = Long.MIN_VALUE;
        previousValue = 0;
        estimatedElementSize = 0;
        estimatedSize = 0;
    }

    @Override
    public void close() {
        super.close();
        this.minDeltaInCurrentBlock = Long.MAX_VALUE;
    }
}