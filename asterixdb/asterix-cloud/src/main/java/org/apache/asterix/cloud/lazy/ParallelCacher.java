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

import static org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager.COMPONENT_FILES_FILTER;

import java.io.FilenameFilter;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A parallel cacher that maintains and downloads (in parallel) all uncached files
 *
 * @see org.apache.asterix.cloud.lazy.accessor.ReplaceableCloudAccessor
 */
public final class ParallelCacher implements IParallelCacher {

    public static final FilenameFilter METADATA_FILTER =
            ((dir, name) -> name.startsWith(StorageConstants.INDEX_NON_DATA_FILES_PREFIX));
    private static final Logger LOGGER = LogManager.getLogger();
    private final IParallelDownloader downloader;
    /**
     * Uncached Indexes subpaths
     */
    private final Set<String> uncachedIndexes;

    /**
     * All uncached data files
     * Example: BTree files
     */
    private final Set<FileReference> uncachedDataFiles;

    /**
     * All uncached metadata files
     * Example: index checkpoint files
     */
    private final Set<FileReference> uncachedMetadataFiles;

    public ParallelCacher(IParallelDownloader downloader, List<FileReference> uncachedFiles) {
        this.downloader = downloader;
        uncachedDataFiles = getFiles(uncachedFiles, COMPONENT_FILES_FILTER);
        uncachedMetadataFiles = getFiles(uncachedFiles, METADATA_FILTER);
        uncachedIndexes = getUncachedIndexes(uncachedFiles);
    }

    @Override
    public boolean isCached(FileReference indexDir) {
        String relativePath = indexDir.getRelativePath();
        if (relativePath.endsWith(StorageConstants.STORAGE_ROOT_DIR_NAME)
                || relativePath.startsWith(StorageConstants.METADATA_TXN_NOWAL_DIR_NAME)) {
            return false;
        }
        String indexSubPath = StoragePathUtil.getIndexSubPath(indexDir, true);
        return !indexSubPath.isEmpty() && !uncachedIndexes.contains(indexSubPath);
    }

    @Override
    public Set<FileReference> getUncachedFiles(FileReference dir, FilenameFilter filter) {
        if (dir.getRelativePath().endsWith(StorageConstants.STORAGE_ROOT_DIR_NAME)) {
            return uncachedDataFiles.stream()
                    .filter(f -> StoragePathUtil.hasSameStorageRoot(dir, f) && filter.accept(null, f.getName()))
                    .collect(Collectors.toSet());
        }
        return uncachedDataFiles
                .stream().filter(f -> StoragePathUtil.hasSameStorageRoot(dir, f)
                        && StoragePathUtil.isRelativeParent(dir, f) && filter.accept(null, f.getName()))
                .collect(Collectors.toSet());
    }

    @Override
    public synchronized boolean downloadData(FileReference indexFile) throws HyracksDataException {
        String indexSubPath = StoragePathUtil.getIndexSubPath(indexFile, false);
        Set<FileReference> toDownload = new HashSet<>();
        for (FileReference fileReference : uncachedDataFiles) {
            if (fileReference.getRelativePath().contains(indexSubPath)) {
                toDownload.add(fileReference.getParent());
            }
        }

        LOGGER.debug("Downloading data files for {} in all partitions: {}", indexSubPath, toDownload);
        Collection<FileReference> failed = downloader.downloadDirectories(toDownload);
        if (!failed.isEmpty()) {
            LOGGER.warn("Failed to download data files {}. Re-downloading: {}", indexSubPath, failed);
            downloader.downloadFiles(failed);
        }
        LOGGER.debug("Finished downloading data files for {}", indexSubPath);
        uncachedIndexes.remove(indexSubPath);
        uncachedDataFiles.removeIf(f -> f.getRelativePath().contains(indexSubPath));
        return isEmpty();
    }

    @Override
    public synchronized boolean downloadMetadata(FileReference indexFile) throws HyracksDataException {
        String indexSubPath = StoragePathUtil.getIndexSubPath(indexFile, false);
        Set<FileReference> toDownload = new HashSet<>();
        for (FileReference fileReference : uncachedMetadataFiles) {
            if (fileReference.getRelativePath().contains(indexSubPath)) {
                toDownload.add(fileReference);
            }
        }

        LOGGER.debug("Downloading metadata files for {} in all partitions: {}", indexSubPath, toDownload);
        downloader.downloadFiles(toDownload);
        LOGGER.debug("Finished downloading metadata files for {}", indexSubPath);
        uncachedMetadataFiles.removeAll(toDownload);
        return isEmpty();
    }

    @Override
    public boolean remove(Collection<FileReference> deletedFiles) {
        LOGGER.info("Deleting {}", deletedFiles);
        for (FileReference fileReference : deletedFiles) {
            remove(fileReference);
        }

        return isEmpty();
    }

    @Override
    public boolean remove(FileReference deletedFile) {
        LOGGER.info("Deleting {}", deletedFile);
        if (COMPONENT_FILES_FILTER.accept(null, deletedFile.getName())) {
            uncachedDataFiles.remove(deletedFile);
        } else {
            uncachedMetadataFiles.remove(deletedFile);
        }

        return isEmpty();
    }

    @Override
    public void close() {
        downloader.close();
        LOGGER.info("Parallel cacher was closed");
    }

    public static Set<FileReference> getFiles(List<FileReference> uncachedFiles, FilenameFilter filter) {
        Set<FileReference> fileReferences = ConcurrentHashMap.newKeySet();
        for (FileReference fileReference : uncachedFiles) {
            if (filter.accept(null, fileReference.getName())) {
                fileReferences.add(fileReference);
            }
        }
        return fileReferences;
    }

    private Set<String> getUncachedIndexes(List<FileReference> uncachedFiles) {
        Set<String> uncachedIndexes = ConcurrentHashMap.newKeySet();
        for (FileReference indexFile : uncachedFiles) {
            uncachedIndexes.add(StoragePathUtil.getIndexSubPath(indexFile, false));
        }
        return uncachedIndexes;
    }

    private synchronized boolean isEmpty() {
        int totalSize = uncachedDataFiles.size() + uncachedMetadataFiles.size();
        LOGGER.info("Current number of uncached files {}", totalSize);
        return totalSize == 0;
    }
}
