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

import java.io.FilenameFilter;
import java.util.Collection;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

public interface IParallelCacher {
    /**
     * Check whether an index file data and metadata are already cached
     *
     * @param indexDir index directory
     * @return true if the index is already cached, false otherwise
     */
    boolean isCached(FileReference indexDir);

    Set<FileReference> getUncachedFiles(FileReference dir, FilenameFilter filter);

    /**
     * Downloads all index's data files for all partitions.
     * The index is inferred from the path of the provided file.
     *
     * @param indexFile a file reference in an index
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean downloadData(FileReference indexFile) throws HyracksDataException;

    /**
     * Downloads all index's metadata files for all partitions.
     * The index is inferred from the path of the provided file.
     *
     * @param indexFile a file reference in an index
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean downloadMetadata(FileReference indexFile) throws HyracksDataException;

    /**
     * Remove the deleted files from the uncached file set
     *
     * @param deletedFiles all deleted files
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean remove(Collection<FileReference> deletedFiles);

    /**
     * Remove the deleted file from the uncached file set
     *
     * @param deletedFile the deleted file
     * @return true if the remaining number of uncached files is zero, false otherwise
     */
    boolean remove(FileReference deletedFile);

    /**
     * Close cacher resources
     */
    void close();
}
