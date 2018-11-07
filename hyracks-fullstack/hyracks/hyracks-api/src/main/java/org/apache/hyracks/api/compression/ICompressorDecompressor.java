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
package org.apache.hyracks.api.compression;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An API for block compressor/decompressor.
 *
 * Note: Should never allocate any buffer in compress/uncompress operations and it must be stateless to be thread safe.
 */
public interface ICompressorDecompressor {
    /**
     * Computes the required buffer size for <i>compress()</i>.
     *
     * @param uBufferSize
     *            The size of the uncompressed buffer.
     * @return The required buffer size for compression
     */
    int computeCompressedBufferSize(int uBufferSize);

    /**
     * Compress <i>uBuffer</i> into <i>cBuffer</i>
     *
     * @param uBuffer
     *            Uncompressed source buffer
     * @param cBuffer
     *            Compressed destination buffer
     * @return Buffer after compression. ({@link ByteBuffer#limit()} is set to the compressed size
     * @throws HyracksDataException
     */
    ByteBuffer compress(ByteBuffer uBuffer, ByteBuffer cBuffer) throws HyracksDataException;

    /**
     * Uncompress <i>cBuffer</i> into <i>uBuffer</i>
     *
     * @param cBuffer
     *            Compressed source buffer
     * @param uBuffer
     *            Uncompressed destination buffer
     * @return Buffer after decompression. ({@link ByteBuffer#limit()} is set to the uncompressed size
     * @throws HyracksDataException
     *             An exception will be thrown if the <i>uBuffer</i> size is not sufficient.
     */
    ByteBuffer uncompress(ByteBuffer cBuffer, ByteBuffer uBuffer) throws HyracksDataException;
}
