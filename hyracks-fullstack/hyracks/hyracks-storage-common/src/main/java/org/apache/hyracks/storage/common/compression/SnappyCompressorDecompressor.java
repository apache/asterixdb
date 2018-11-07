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
package org.apache.hyracks.storage.common.compression;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.xerial.snappy.Snappy;

/**
 * Built-in Snappy compressor/decompressor wrapper
 */
public class SnappyCompressorDecompressor implements ICompressorDecompressor {
    protected static final SnappyCompressorDecompressor INSTANCE = new SnappyCompressorDecompressor();

    private SnappyCompressorDecompressor() {

    }

    @Override
    public int computeCompressedBufferSize(int uncompressedBufferSize) {
        return Snappy.maxCompressedLength(uncompressedBufferSize);
    }

    @Override
    public ByteBuffer compress(ByteBuffer uBuffer, ByteBuffer cBuffer) throws HyracksDataException {
        try {
            final int cLength = Snappy.compress(uBuffer.array(), uBuffer.position(), uBuffer.remaining(),
                    cBuffer.array(), cBuffer.position());
            cBuffer.limit(cBuffer.position() + cLength);
            return cBuffer;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public ByteBuffer uncompress(ByteBuffer cBuffer, ByteBuffer uBuffer) throws HyracksDataException {
        try {
            final int uLength = Snappy.uncompress(cBuffer.array(), cBuffer.position(), cBuffer.remaining(),
                    uBuffer.array(), uBuffer.position());
            uBuffer.limit(uBuffer.position() + uLength);
            return uBuffer;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
