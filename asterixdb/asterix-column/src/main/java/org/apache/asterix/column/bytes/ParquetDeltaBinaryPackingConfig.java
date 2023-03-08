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
package org.apache.asterix.column.bytes;

import java.io.IOException;
import java.io.InputStream;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;

/**
 * Copy of {@link org.apache.parquet.column.values.delta.DeltaBinaryPackingConfig}
 */
public class ParquetDeltaBinaryPackingConfig {
    private int blockSizeInValues;
    private int miniBlockNumInABlock;
    private int miniBlockSizeInValues;

    public ParquetDeltaBinaryPackingConfig(int blockSizeInValues, int miniBlockNumInABlock) {
        reset(blockSizeInValues, miniBlockNumInABlock);
    }

    private void reset(int blockSizeInValues, int miniBlockNumInABlock) {
        this.blockSizeInValues = blockSizeInValues;
        this.miniBlockNumInABlock = miniBlockNumInABlock;
        double miniSize = (double) blockSizeInValues / miniBlockNumInABlock;
        Preconditions.checkArgument(miniSize % 8 == 0, "miniBlockSize must be multiple of 8, but it's " + miniSize);
        this.miniBlockSizeInValues = (int) miniSize;
    }

    public static ParquetDeltaBinaryPackingConfig readConfig(InputStream in, ParquetDeltaBinaryPackingConfig config)
            throws IOException {
        final int blockSizeInValues = BytesUtils.readUnsignedVarInt(in);
        final int miniBlockNumInABlock = BytesUtils.readUnsignedVarInt(in);
        if (config == null) {
            return new ParquetDeltaBinaryPackingConfig(blockSizeInValues, miniBlockNumInABlock);
        }
        config.reset(blockSizeInValues, miniBlockNumInABlock);
        return config;
    }

    public BytesInput toBytesInput() {
        return BytesInput.concat(BytesInput.fromUnsignedVarInt(blockSizeInValues),
                BytesInput.fromUnsignedVarInt(miniBlockNumInABlock));
    }

    public int getBlockSizeInValues() {
        return blockSizeInValues;
    }

    public int getMiniBlockNumInABlock() {
        return miniBlockNumInABlock;
    }

    public int getMiniBlockSizeInValues() {
        return miniBlockSizeInValues;
    }
}
