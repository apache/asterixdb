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
package org.apache.asterix.cloud.clients;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A cloud-based writer that write bytes sequentially in a cloud blob storage
 */
public interface ICloudWriter {
    /**
     * Write a header and a page
     *
     * @param header to write
     * @param page   to write
     * @return written bytes
     */
    int write(ByteBuffer header, ByteBuffer page) throws HyracksDataException;

    /**
     * Write a page
     *
     * @param page to write
     * @return written bytes
     */
    int write(ByteBuffer page) throws HyracksDataException;

    /**
     * Write a byte
     *
     * @param b to write
     */
    void write(int b) throws HyracksDataException;

    /**
     * Write a byte array
     *
     * @param b   bytes to write
     * @param off starting offset
     * @param len length to write
     * @return written bytes
     */
    int write(byte[] b, int off, int len) throws HyracksDataException;

    /**
     * Finish the write operation
     * Note: this should be called upon successful write
     */
    void finish() throws HyracksDataException;

    /**
     * Abort the write operation
     * Note: should be called instead of {@link #finish()} when the write operation encountered an error
     */
    void abort() throws HyracksDataException;
}
