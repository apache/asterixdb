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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressor;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.NotThreadSafe;

@NotThreadSafe
public abstract class AbstractLSMIndexFileManager implements ILSMIndexFileManager {

    public enum TreeIndexState {
        INVALID,
        VERSION_MISMATCH,
        VALID
    }

    /**
     * Split different parts of the file name
     */
    public static final String DELIMITER = "_";
    /**
     * Indicates a B tree
     */
    public static final String BTREE_SUFFIX = "b";
    /**
     * Indicates an R tree
     */
    public static final String RTREE_SUFFIX = "r";
    /**
     * Indicates a bloom filter
     */
    public static final String BLOOM_FILTER_SUFFIX = "f";
    /**
     * Indicates a delete tree
     */
    public static final String DELETE_TREE_SUFFIX = "d";
    /**
     * Indicates Look Aside File (LAF) for compressed indexes
     */
    public static final String LAF_SUFFIX = ".dic";
    public static final long UNINITIALIZED_COMPONENT_SEQ = -1;
    public static final FilenameFilter COMPONENT_FILES_FILTER = (dir, name) -> !name.startsWith(".");
    protected static FilenameFilter BLOOM_FILTER_FILTER =
            (dir, name) -> !name.startsWith(".") && name.endsWith(BLOOM_FILTER_SUFFIX);
    protected static FilenameFilter LAF_FILTER = (dir, name) -> !name.startsWith(".") && name.endsWith(LAF_SUFFIX);
    protected static final Comparator<String> cmp = new FileNameComparator();

    protected final IIOManager ioManager;
    // baseDir should reflect dataset name and partition name and be absolute
    protected final FileReference baseDir;
    protected final Comparator<IndexComponentFileReference> recencyCmp = new RecencyComparator();
    protected final TreeIndexFactory<? extends ITreeIndex> treeFactory;
    private long lastUsedComponentSeq = UNINITIALIZED_COMPONENT_SEQ;
    private final ICompressorDecompressorFactory compressorDecompressorFactory;

    public AbstractLSMIndexFileManager(IIOManager ioManager, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> treeFactory) {
        this(ioManager, file, treeFactory, NoOpCompressorDecompressorFactory.INSTANCE);
    }

    public AbstractLSMIndexFileManager(IIOManager ioManager, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> treeFactory,
            ICompressorDecompressorFactory compressorDecompressorFactory) {
        this.ioManager = ioManager;
        this.baseDir = file;
        this.treeFactory = treeFactory;
        this.compressorDecompressorFactory = compressorDecompressorFactory;
    }

    protected TreeIndexState isValidTreeIndex(ITreeIndex treeIndex) throws HyracksDataException {
        IBufferCache bufferCache = treeIndex.getBufferCache();
        treeIndex.activate();
        try {
            int metadataPage = treeIndex.getPageManager().getMetadataPageId();
            if (metadataPage < 0) {
                return TreeIndexState.INVALID;
            }
            ITreeIndexMetadataFrame metadataFrame = treeIndex.getPageManager().createMetadataFrame();
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(treeIndex.getFileId(), metadataPage));
            page.acquireReadLatch();
            try {
                metadataFrame.setPage(page);
                if (!metadataFrame.isValid()) {
                    return TreeIndexState.INVALID;
                } else if (metadataFrame.getVersion() != ITreeIndexFrame.Constants.VERSION) {
                    return TreeIndexState.VERSION_MISMATCH;
                } else {
                    return TreeIndexState.VALID;
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        } finally {
            treeIndex.deactivate();
        }
    }

    protected void cleanupAndGetValidFilesInternal(FilenameFilter filter,
            TreeIndexFactory<? extends ITreeIndex> treeFactory, List<IndexComponentFileReference> allFiles,
            IBufferCache bufferCache) throws HyracksDataException {
        Set<FileReference> files = ioManager.list(baseDir, filter);
        for (FileReference filePath : files) {
            FileReference fileRef = getCompressedFileReferenceIfAny(filePath.getName());
            if (treeFactory == null) {
                allFiles.add(IndexComponentFileReference.of(fileRef));
                continue;
            }
            TreeIndexState idxState = isValidTreeIndex(treeFactory.createIndexInstance(fileRef));
            if (idxState == TreeIndexState.VALID) {
                allFiles.add(IndexComponentFileReference.of(fileRef));
            } else if (idxState == TreeIndexState.INVALID) {
                bufferCache.deleteFile(fileRef);
            }
        }
    }

    protected void validateFiles(Set<String> groundTruth, List<IndexComponentFileReference> validFiles,
            FilenameFilter filter, TreeIndexFactory<? extends ITreeIndex> treeFactory, IBufferCache bufferCache)
            throws HyracksDataException {
        ArrayList<IndexComponentFileReference> tmpAllInvListsFiles = new ArrayList<>();
        cleanupAndGetValidFilesInternal(filter, treeFactory, tmpAllInvListsFiles, bufferCache);
        for (IndexComponentFileReference cmpFileName : tmpAllInvListsFiles) {
            if (groundTruth.contains(cmpFileName.getSequence())) {
                validFiles.add(cmpFileName);
            } else {
                delete(bufferCache, cmpFileName.getFileRef());
            }
        }
    }

    @Override
    public void createDirs() throws HyracksDataException {
        if (ioManager.exists(baseDir)) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_EXISTING_INDEX);
        }
        ioManager.makeDirectories(baseDir);
    }

    @Override
    public void deleteDirs() throws HyracksDataException {
        ioManager.delete(baseDir);
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() throws HyracksDataException {
        final String sequence = getNextComponentSequence(COMPONENT_FILES_FILTER);
        return new LSMComponentFileReferences(baseDir.getChild(sequence), null, null);
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName) {
        final String baseName = IndexComponentFileReference.getMergeSequence(firstFileName, lastFileName);
        return new LSMComponentFileReferences(baseDir.getChild(baseName), null, null);
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<>();
        ArrayList<IndexComponentFileReference> allFiles = new ArrayList<>();

        // Gather files and delete invalid files
        // There are two types of invalid files:
        // (1) The isValid flag is not set
        // (2) The file's interval is contained by some other file
        // Here, we only filter out (1).
        cleanupAndGetValidFilesInternal(COMPONENT_FILES_FILTER, treeFactory, allFiles, treeFactory.getBufferCache());

        if (allFiles.isEmpty()) {
            return validFiles;
        }

        if (allFiles.size() == 1) {
            validFiles.add(new LSMComponentFileReferences(allFiles.get(0).getFileRef(), null, null));
            return validFiles;
        }

        // Sorts files names from earliest to latest
        Collections.sort(allFiles);

        List<IndexComponentFileReference> validComparableFiles = new ArrayList<>();
        IndexComponentFileReference last = allFiles.get(0);
        validComparableFiles.add(last);
        for (int i = 1; i < allFiles.size(); i++) {
            IndexComponentFileReference current = allFiles.get(i);
            if (current.isMoreRecentThan(last)) {
                // The current start sequence is greater than last stop sequence so current is valid.
                validComparableFiles.add(current);
                last = current;
            } else if (current.isWithin(last)) {
                // The current file is completely contained in the interval of the
                // last file. Thus the last file must contain at least as much information
                // as the current file, so delete the current file.
                delete(treeFactory.getBufferCache(), current.getFileRef());
            } else {
                // This scenario should not be possible since timestamps are monotonically increasing.
                throw HyracksDataException.create(ErrorCode.FOUND_OVERLAPPING_LSM_FILES, baseDir);
            }
        }
        // Sort valid files in reverse lexicographical order, such that newer files come first.
        validComparableFiles.sort(recencyCmp);
        for (IndexComponentFileReference cmpFileName : validComparableFiles) {
            validFiles.add(new LSMComponentFileReferences(cmpFileName.getFileRef(), null, null));
        }
        return validFiles;
    }

    @Override
    public Comparator<String> getFileNameComparator() {
        return cmp;
    }

    /**
     * Sorts strings in reverse lexicographical order. The way we construct the
     * file names above guarantees that:
     * 1. Flushed files sort lower than merged files
     * 2. Flushed files are sorted from newest to oldest (based on the timestamp
     * string)
     */
    private static class FileNameComparator implements Comparator<String> {
        @Override
        public int compare(String a, String b) {
            return IndexComponentFileReference.of(b).compareTo(IndexComponentFileReference.of(a));
        }
    }

    @Override
    public FileReference getBaseDir() {
        return baseDir;
    }

    private static class RecencyComparator implements Comparator<IndexComponentFileReference> {
        @Override
        public int compare(IndexComponentFileReference a, IndexComponentFileReference b) {
            int startCmp = -Long.compare(a.getSequenceStart(), b.getSequenceStart());
            if (startCmp != 0) {
                return startCmp;
            }
            return -Long.compare(a.getSequenceEnd(), b.getSequenceEnd());
        }
    }

    @Override
    public void initLastUsedSeq(long lastUsedSeq) {
        lastUsedComponentSeq = lastUsedSeq;
    }

    protected void delete(IBufferCache bufferCache, FileReference fileRef) throws HyracksDataException {
        bufferCache.deleteFile(fileRef);
    }

    protected String getNextComponentSequence(FilenameFilter filenameFilter) throws HyracksDataException {
        if (lastUsedComponentSeq == UNINITIALIZED_COMPONENT_SEQ) {
            lastUsedComponentSeq = getOnDiskLastUsedComponentSequence(filenameFilter);
        }
        return IndexComponentFileReference.getFlushSequence(++lastUsedComponentSeq);
    }

    protected FileReference getCompressedFileReferenceIfAny(String name) {
        final ICompressorDecompressor compDecomp = compressorDecompressorFactory.createInstance();
        FileReference treeFileRef;
        //Avoid creating LAF file for NoOpCompressorDecompressor
        if (compDecomp != NoOpCompressorDecompressor.INSTANCE && isCompressible(name)) {
            final String path = baseDir.getChildPath(name);
            treeFileRef = new CompressedFileReference(baseDir.getDeviceHandle(), compDecomp, path, path + LAF_SUFFIX);
        } else {
            treeFileRef = baseDir.getChild(name);
        }

        if (areHolesAllowed()) {
            treeFileRef.setHolesAllowed();
        }

        return treeFileRef;
    }

    protected void cleanLookAsideFiles(Set<String> groundTruth, IBufferCache bufferCache) throws HyracksDataException {
        ICompressorDecompressor compDecomp = compressorDecompressorFactory.createInstance();
        if (compDecomp == NoOpCompressorDecompressor.INSTANCE) {
            return;
        }

        List<IndexComponentFileReference> allLookAsideFiles = new ArrayList<>();
        cleanupAndGetValidFilesInternal(LAF_FILTER, null, allLookAsideFiles, null);
        for (IndexComponentFileReference laf : allLookAsideFiles) {
            if (!groundTruth.contains(laf.getSequence())) {
                delete(bufferCache, laf.getFileRef());
            }
        }
    }

    protected boolean areHolesAllowed() {
        return false;
    }

    private boolean isCompressible(String fileName) {
        return !fileName.endsWith(BLOOM_FILTER_SUFFIX) && !fileName.endsWith(DELETE_TREE_SUFFIX);
    }

    private long getOnDiskLastUsedComponentSequence(FilenameFilter filenameFilter) throws HyracksDataException {
        long maxComponentSeq = -1;
        final Set<FileReference> files = ioManager.list(baseDir, filenameFilter);
        for (FileReference file : files) {
            maxComponentSeq = Math.max(maxComponentSeq, IndexComponentFileReference.of(file).getSequenceEnd());
        }
        return maxComponentSeq;
    }
}
