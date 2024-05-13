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
package org.apache.asterix.cloud.lazy.accessor;

import java.io.FilenameFilter;
import java.util.Set;

import org.apache.asterix.cloud.CloudFileHandle;
import org.apache.asterix.cloud.bulk.IBulkOperationCallBack;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;

/**
 * An abstraction for lazy I/O operations
 */
public interface ILazyAccessor {
    /**
     * @return whether this is local accessor
     */
    boolean isLocalAccessor();

    /**
     * @return a callback for bulk operation
     */
    IBulkOperationCallBack getBulkOperationCallBack();

    /**
     * Notify opening a file
     *
     * @param fileHandle to open
     */
    void doOnOpen(CloudFileHandle fileHandle) throws HyracksDataException;

    /**
     * List a directory
     *
     * @param dir    to list
     * @param filter filter to return only specific file
     * @return set of all files that in a directory and satisfy the filter
     */
    Set<FileReference> doList(FileReference dir, FilenameFilter filter) throws HyracksDataException;

    /**
     * Checks whether a file exits
     *
     * @param fileRef to check
     * @return true if exists, false otherwise
     */
    boolean doExists(FileReference fileRef) throws HyracksDataException;

    /**
     * Get a size of a file
     *
     * @param fileReference to get the size of
     * @return size in bytes
     */
    long doGetSize(FileReference fileReference) throws HyracksDataException;

    /**
     * Read all bytes of a file
     *
     * @param fileReference to read
     * @return read bytes
     */
    byte[] doReadAllBytes(FileReference fileReference) throws HyracksDataException;

    /**
     * Delete a file
     *
     * @param fileReference to delete
     */
    void doDelete(FileReference fileReference) throws HyracksDataException;

    /**
     * Overwrite a file with the provided data
     *
     * @param fileReference to overwrite
     * @param bytes         to be written
     */
    void doOverwrite(FileReference fileReference, byte[] bytes) throws HyracksDataException;

    /**
     * Punch a hole in a sweepable file (only)
     *
     * @param fileHandle file handle
     * @param offset     starting offset
     * @param length     length
     */
    int doPunchHole(IFileHandle fileHandle, long offset, long length) throws HyracksDataException;

    /**
     * Evicts a directory deletes it only in the local drive
     *
     * @param directory to evict
     */
    void doEvict(FileReference directory) throws HyracksDataException;
}
