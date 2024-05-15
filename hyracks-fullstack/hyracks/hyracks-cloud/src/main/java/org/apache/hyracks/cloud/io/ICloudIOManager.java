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
package org.apache.hyracks.cloud.io;

import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;

/**
 * Certain operations needed to be provided by {@link org.apache.hyracks.api.io.IIOManager} to support cloud
 * file operations in a cloud deployment.
 */
public interface ICloudIOManager {
    /**
     * Read from the cloud
     *
     * @param fHandle file handle
     * @param offset  starting offset
     * @param data    buffer to read to
     */
    void cloudRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    /**
     * Read from the cloud
     *
     * @param fHandle file handle
     * @param offset  starting offset
     * @return input stream of the required data
     */
    InputStream cloudRead(IFileHandle fHandle, long offset, long length);

    /**
     * Write to local drive only
     *
     * @param fHandle file handle
     * @param offset  starting offset
     * @param data    to write
     */

    int localWriter(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    /**
     * Write to cloud only
     *
     * @param fHandle file handle
     * @param data    to write
     * @return number of written bytes
     */
    int cloudWrite(IFileHandle fHandle, ByteBuffer data) throws HyracksDataException;

    /**
     * Write to cloud only
     *
     * @param fHandle file handle
     * @param data    to write
     * @return number of written bytes
     */
    long cloudWrite(IFileHandle fHandle, ByteBuffer[] data) throws HyracksDataException;

    /**
     * Punch a hole in a file
     *
     * @param fHandle file handle
     * @param offset  starting offset
     * @param length  length
     */
    int punchHole(IFileHandle fHandle, long offset, long length) throws HyracksDataException;

    /**
     * Evict a resource from the local disk cache
     *
     * @param resourcePath to evict
     */
    void evict(String resourcePath) throws HyracksDataException;
}
