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
package org.apache.asterix.cloud.lazy;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Collection;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

public interface IParallelCacher {
    /**
     * Check whether a file is cacheable or not
     *
     * @param fileReference file
     * @return true if the file is already cached, false otherwise
     */
    boolean isCacheable(FileReference fileReference);

    /**
     * Returns a list of all uncached files
     *
     * @param dir    directory to list
     * @param filter file name filter
     * @return set of uncached files
     */
    Set<FileReference> getUncachedFiles(FileReference dir, FilenameFilter filter);

    /**
     * Returns the size of a file
     *
     * @param fileReference file
     * @return the size of the file if it exists or zero otherwise (as expected when calling {@link File#length()})
     */
    long getSize(FileReference fileReference);

    /**
     * @return total size of uncached files
     */
    long getUncachedTotalSize();

    /**
     * Creates empty data files
     *
     * @param indexFile a file reference in an index
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean createEmptyDataFiles(FileReference indexFile) throws HyracksDataException;

    /**
     * Downloads all index's data files for all partitions.
     * The index is inferred from the path of the provided file.
     *
     * @param indexFile a file reference in an index
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean downloadDataFiles(FileReference indexFile) throws HyracksDataException;

    /**
     * Downloads all index's metadata files for all partitions.
     * The index is inferred from the path of the provided file.
     *
     * @param indexFile a file reference in an index
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean downloadMetadataFiles(FileReference indexFile) throws HyracksDataException;

    /**
     * Remove the deleted files from the uncached file set
     *
     * @param deletedFiles all deleted files
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean remove(Collection<FileReference> deletedFiles);

    /**
     * Remove a file from the uncached file set
     *
     * @param fileReference the deleted file
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean remove(FileReference fileReference);

    /**
     * Add files to indicated that they are not cached anymore
     *
     * @param files to be uncached
     */
    void add(Collection<FileReference> files);

    /**
     * Close cacher resources
     */
    void close() throws HyracksDataException;
}
