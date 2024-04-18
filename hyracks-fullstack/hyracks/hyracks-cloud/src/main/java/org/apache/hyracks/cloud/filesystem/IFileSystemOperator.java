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
package org.apache.hyracks.cloud.filesystem;

import java.nio.channels.FileChannel;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An interface for native file system operations
 */
interface IFileSystemOperator {
    /**
     * Returns the file descriptor of a file
     *
     * @param fileChannel opened file channel
     * @return file descriptor as reported by the OS
     */
    int getFileDescriptor(FileChannel fileChannel) throws HyracksDataException;

    /**
     * Block size of a file (OS dependent)
     *
     * @param fileDescriptor of the file
     * @return block size
     */
    int getBlockSize(int fileDescriptor) throws HyracksDataException;

    /**
     * Punches a hole in a file
     *
     * @param fileDescriptor of the file
     * @param offset         starting offset
     * @param length         length
     * @param blockSize      block size
     * @return length of the hole
     */
    long punchHole(int fileDescriptor, long offset, long length, int blockSize) throws HyracksDataException;
}
