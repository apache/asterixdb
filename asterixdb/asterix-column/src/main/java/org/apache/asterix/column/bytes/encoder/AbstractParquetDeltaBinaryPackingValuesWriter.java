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

import org.apache.asterix.column.bytes.ParquetDeltaBinaryPackingConfig;
import org.apache.asterix.column.bytes.stream.out.MultiTemporaryBufferBytesOutputStream;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;

/**
 * Re-implementation of {@link DeltaBinaryPackingValuesWriter}
 */
public abstract class AbstractParquetDeltaBinaryPackingValuesWriter extends AbstractParquetValuesWriter {

    public static final int DEFAULT_NUM_BLOCK_VALUES = 128;

    public static final int DEFAULT_NUM_MINIBLOCKS = 4;

    protected final MultiTemporaryBufferBytesOutputStream outputStream;

    /**
     * stores blockSizeInValues, miniBlockNumInABlock and miniBlockSizeInValues
     */
    protected final ParquetDeltaBinaryPackingConfig config;

    /**
     * bit width for each mini block, reused between flushes
     */
    protected final int[] bitWidths;

    protected int totalValueCount = 0;

    /**
     * a pointer to deltaBlockBuffer indicating the end of deltaBlockBuffer
     * the number of values in the deltaBlockBuffer that haven't flushed to baos
     * it will be reset after each flush
     */
    protected int deltaValuesToFlush = 0;

    /**
     * bytes buffer for a mini block, it is reused for each mini block.
     * Therefore the size of biggest miniblock with bitwith of MAX_BITWITH is allocated
     */
    protected byte[] miniBlockByteBuffer;

    /**
     * Estimated element size after encoding
     */
    protected int estimatedElementSize = 0;
    /**
     * Estimated size for all non-flushed elements
     */
    protected int estimatedSize = 0;

    protected AbstractParquetDeltaBinaryPackingValuesWriter(int blockSizeInValues, int miniBlockNum,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        this.config = new ParquetDeltaBinaryPackingConfig(blockSizeInValues, miniBlockNum);
        bitWidths = new int[config.getMiniBlockNumInABlock()];
        outputStream = new MultiTemporaryBufferBytesOutputStream(multiPageOpRef);
    }

    protected void writeBitWidthForMiniBlock(int i) {
        try {
            BytesUtils.writeIntLittleEndianOnOneByte(outputStream, bitWidths[i]);
        } catch (IOException e) {
            throw new ParquetEncodingException("can not write bit width for mini-block", e);
        }
    }

    protected int getMiniBlockCountToFlush(double numberCount) {
        return (int) Math.ceil(numberCount / config.getMiniBlockSizeInValues());
    }

    @Override
    public void reset() throws HyracksDataException {
        this.totalValueCount = 0;
        this.outputStream.reset();
        this.deltaValuesToFlush = 0;
    }

    @Override
    public void close() {
        this.totalValueCount = 0;
        this.deltaValuesToFlush = 0;
        outputStream.finish();
    }

    @Override
    public int getEstimatedSize() {
        return outputStream.size() + estimatedSize;
    }

    @Override
    public int getAllocatedSize() {
        return outputStream.capacity();
    }
}
