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

import static org.apache.asterix.cloud.util.CloudFileUtil.DATA_FILTER;
import static org.apache.asterix.cloud.util.CloudFileUtil.METADATA_FILTER;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.asterix.cloud.UncachedFileReference;
import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A parallel cacher that maintains and downloads (in parallel) all uncached files
 *
 * @see org.apache.asterix.cloud.lazy.accessor.ReplaceableCloudAccessor
 * @see org.apache.asterix.cloud.lazy.accessor.SelectiveCloudAccessor
 */
public final class ParallelCacher implements IParallelCacher {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IParallelDownloader downloader;
    /**
     * Uncached Indexes subpaths
     */
    private final Set<String> uncachedIndexes;
    private final boolean checkEmpty;

    /**
     * All uncached data files
     * Example: BTree files
     */
    private final Map<FileReference, UncachedFileReference> uncachedDataFiles;

    /**
     * All uncached metadata files
     * Example: index checkpoint files
     */
    private final Map<FileReference, UncachedFileReference> uncachedMetadataFiles;

    public ParallelCacher(IParallelDownloader downloader, List<FileReference> uncachedFiles, boolean checkEmpty) {
        this.downloader = downloader;
        uncachedDataFiles = new ConcurrentHashMap<>(getFiles(uncachedFiles, DATA_FILTER));
        uncachedMetadataFiles = new ConcurrentHashMap<>(getFiles(uncachedFiles, METADATA_FILTER));
        uncachedIndexes = getUncachedIndexes(uncachedFiles);
        this.checkEmpty = checkEmpty;
    }

    @Override
    public boolean isCacheable(FileReference fileReference) {
        if (isDataFile(fileReference)) {
            return uncachedDataFiles.containsKey(fileReference);
        } else {
            return uncachedMetadataFiles.containsKey(fileReference);
        }
    }

    @Override
    public Set<FileReference> getUncachedFiles(FileReference dir, FilenameFilter filter) {
        if (dir.getRelativePath().endsWith(StorageConstants.STORAGE_ROOT_DIR_NAME)) {
            return uncachedDataFiles.keySet().stream()
                    .filter(f -> StoragePathUtil.hasSameStorageRoot(dir, f) && filter.accept(null, f.getName()))
                    .collect(Collectors.toSet());
        }
        return uncachedDataFiles.keySet()
                .stream().filter(f -> StoragePathUtil.hasSameStorageRoot(dir, f)
                        && StoragePathUtil.isRelativeParent(dir, f) && filter.accept(null, f.getName()))
                .collect(Collectors.toSet());
    }

    @Override
    public long getSize(FileReference fileReference) {
        UncachedFileReference uncachedFile;
        if (isDataFile(fileReference)) {
            uncachedFile = uncachedDataFiles.get(fileReference);
        } else {
            uncachedFile = uncachedMetadataFiles.get(fileReference);
        }

        return uncachedFile == null ? 0L : uncachedFile.getSize();
    }

    @Override
    public long getUncachedTotalSize() {
        return getTotalSize(uncachedMetadataFiles.values()) + getTotalSize(uncachedDataFiles.values());
    }

    @Override
    public synchronized boolean createEmptyDataFiles(FileReference indexFile) throws HyracksDataException {
        String indexSubPath = StoragePathUtil.getIndexSubPath(indexFile, false);
        Set<FileReference> toCreate = getForAllPartitions(uncachedDataFiles.values(), indexSubPath);

        LOGGER.debug("Creating empty data files for {} in all partitions: {}", indexSubPath, toCreate);
        createEmptyFiles(toCreate);
        LOGGER.debug("Finished creating data files for {}", indexSubPath);
        uncachedIndexes.remove(indexSubPath);
        uncachedDataFiles.keySet().removeIf(f -> f.getRelativePath().contains(indexSubPath));
        return isEmpty();
    }

    @Override
    public synchronized boolean downloadDataFiles(FileReference indexFile) throws HyracksDataException {
        String indexSubPath = StoragePathUtil.getIndexSubPath(indexFile, false);
        Set<FileReference> toDownload = getForAllPartitions(uncachedDataFiles.values(), indexSubPath);

        LOGGER.debug("Downloading data files for {} in all partitions: {}", indexSubPath, toDownload);
        downloader.downloadFiles(toDownload);
        LOGGER.debug("Finished downloading data files for {}", indexSubPath);
        uncachedIndexes.remove(indexSubPath);
        uncachedDataFiles.keySet().removeIf(f -> f.getRelativePath().contains(indexSubPath));
        return isEmpty();
    }

    @Override
    public synchronized boolean downloadMetadataFiles(FileReference indexFile) throws HyracksDataException {
        String indexSubPath = StoragePathUtil.getIndexSubPath(indexFile, false);
        Set<FileReference> toDownload = getForAllPartitions(uncachedMetadataFiles.keySet(), indexSubPath);

        LOGGER.debug("Downloading metadata files for {} in all partitions: {}", indexSubPath, toDownload);
        downloader.downloadFiles(toDownload);
        LOGGER.debug("Finished downloading metadata files for {}", indexSubPath);
        uncachedMetadataFiles.keySet().removeAll(toDownload);
        return isEmpty();
    }

    @Override
    public boolean remove(Collection<FileReference> deletedFiles) {
        if (!deletedFiles.isEmpty()) {
            LOGGER.info("Deleting {}", deletedFiles);
        }

        for (FileReference fileReference : deletedFiles) {
            remove(fileReference);
        }

        return isEmpty();
    }

    @Override
    public boolean remove(FileReference fileReference) {
        LOGGER.info("Deleting {}", fileReference);
        if (isDataFile(fileReference)) {
            uncachedDataFiles.remove(fileReference);
        } else {
            uncachedMetadataFiles.remove(fileReference);
        }

        return isEmpty();
    }

    @Override
    public synchronized void add(Collection<FileReference> files) {
        LOGGER.info("Uncache {}", files);
        // We only can 'uncache' data files
        uncachedDataFiles.putAll(getFiles(files, DATA_FILTER));
    }

    @Override
    public void close() throws HyracksDataException {
        downloader.close();
        LOGGER.info("Parallel cacher was closed");
    }

    public static Map<FileReference, UncachedFileReference> getFiles(Collection<FileReference> uncachedFiles,
            FilenameFilter filter) {
        Map<FileReference, UncachedFileReference> fileReferences = new HashMap<>();
        for (FileReference fileReference : uncachedFiles) {
            if (filter.accept(null, fileReference.getName())) {
                fileReferences.put(fileReference, (UncachedFileReference) fileReference);
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

    private boolean isDataFile(FileReference fileReference) {
        return DATA_FILTER.accept(null, fileReference.getName());
    }

    private synchronized boolean isEmpty() {
        if (!checkEmpty) {
            return false;
        }
        int totalSize = uncachedDataFiles.size() + uncachedMetadataFiles.size();
        LOGGER.info("Current number of uncached files {}", totalSize);
        return totalSize == 0;
    }

    private static Set<FileReference> getForAllPartitions(Collection<? extends FileReference> uncachedFiles,
            String indexSubPath) {
        Set<FileReference> allFiles = new HashSet<>();
        for (FileReference fileReference : uncachedFiles) {
            if (fileReference.getRelativePath().contains(indexSubPath)) {
                allFiles.add(fileReference);
            }
        }

        return allFiles;
    }

    private static long getTotalSize(Collection<UncachedFileReference> fileReferences) {
        long size = 0L;
        for (UncachedFileReference uncached : fileReferences) {
            size += uncached.getSize();
        }
        return size;
    }

    private static void createEmptyFiles(Set<FileReference> toCreate) throws HyracksDataException {
        for (FileReference fileReference : toCreate) {
            IoUtil.create(fileReference);
            try (RandomAccessFile raf = new RandomAccessFile(fileReference.getAbsolutePath(), "rw")) {
                raf.setLength(((UncachedFileReference) fileReference).getSize());
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

}
