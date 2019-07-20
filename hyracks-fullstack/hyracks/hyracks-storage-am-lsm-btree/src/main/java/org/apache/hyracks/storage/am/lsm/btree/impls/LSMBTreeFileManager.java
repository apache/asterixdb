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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;

public class LSMBTreeFileManager extends AbstractLSMIndexFileManager {

    private static final FilenameFilter btreeFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(BTREE_SUFFIX);
    private final TreeIndexFactory<? extends ITreeIndex> btreeFactory;
    private final boolean hasBloomFilter;

    public LSMBTreeFileManager(IIOManager ioManager, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> btreeFactory, boolean hasBloomFilter,
            ICompressorDecompressorFactory compressorDecompressorFactory) {
        super(ioManager, file, null, compressorDecompressorFactory);
        this.btreeFactory = btreeFactory;
        this.hasBloomFilter = hasBloomFilter;
    }

    public LSMBTreeFileManager(IIOManager ioManager, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> btreeFactory, boolean hasBloomFilter) {
        this(ioManager, file, btreeFactory, hasBloomFilter, NoOpCompressorDecompressorFactory.INSTANCE);
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() throws HyracksDataException {
        String baseName = getNextComponentSequence(btreeFilter);
        return new LSMComponentFileReferences(getFileReference(baseName + DELIMITER + BTREE_SUFFIX), null,
                hasBloomFilter ? getFileReference(baseName + DELIMITER + BLOOM_FILTER_SUFFIX) : null);
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName) {
        final String baseName = IndexComponentFileReference.getMergeSequence(firstFileName, lastFileName);
        return new LSMComponentFileReferences(getFileReference(baseName + DELIMITER + BTREE_SUFFIX), null,
                hasBloomFilter ? getFileReference(baseName + DELIMITER + BLOOM_FILTER_SUFFIX) : null);
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<>();
        ArrayList<IndexComponentFileReference> allBTreeFiles = new ArrayList<>();
        ArrayList<IndexComponentFileReference> allBloomFilterFiles = new ArrayList<>();
        // create transaction filter <to hide transaction files>
        FilenameFilter transactionFilter = getTransactionFileFilter(false);
        // List of valid BTree files.
        cleanupAndGetValidFilesInternal(getCompoundFilter(transactionFilter, btreeFilter), btreeFactory, allBTreeFiles,
                btreeFactory.getBufferCache());
        HashSet<String> btreeFilesSet = new HashSet<>();
        for (IndexComponentFileReference cmpFileName : allBTreeFiles) {
            int index = cmpFileName.getFileName().lastIndexOf(DELIMITER);
            btreeFilesSet.add(cmpFileName.getFileName().substring(0, index));
        }

        if (hasBloomFilter) {
            validateFiles(btreeFilesSet, allBloomFilterFiles, getCompoundFilter(transactionFilter, bloomFilterFilter),
                    null, btreeFactory.getBufferCache());
            // Sanity check.
            if (allBTreeFiles.size() != allBloomFilterFiles.size()) {
                throw HyracksDataException.create(ErrorCode.UNEQUAL_NUM_FILTERS_TREES, baseDir);
            }
        }

        // Trivial cases.
        if (allBTreeFiles.isEmpty() || hasBloomFilter && allBloomFilterFiles.isEmpty()) {
            return validFiles;
        }

        // Special case: sorting is not required
        if (allBTreeFiles.size() == 1 && (!hasBloomFilter || allBloomFilterFiles.size() == 1)) {
            validFiles.add(new LSMComponentFileReferences(allBTreeFiles.get(0).getFileRef(), null,
                    hasBloomFilter ? allBloomFilterFiles.get(0).getFileRef() : null));
            return validFiles;
        }

        // Sorts files names from earliest to latest sequence.
        Collections.sort(allBTreeFiles);
        if (hasBloomFilter) {
            Collections.sort(allBloomFilterFiles);
        }

        List<IndexComponentFileReference> validComparableBTreeFiles = new ArrayList<>();
        IndexComponentFileReference lastBTree = allBTreeFiles.get(0);
        validComparableBTreeFiles.add(lastBTree);

        List<IndexComponentFileReference> validComparableBloomFilterFiles = null;
        IndexComponentFileReference lastBloomFilter = null;
        if (hasBloomFilter) {
            validComparableBloomFilterFiles = new ArrayList<>();
            lastBloomFilter = allBloomFilterFiles.get(0);
            validComparableBloomFilterFiles.add(lastBloomFilter);
        }

        IndexComponentFileReference currentBTree;
        IndexComponentFileReference currentBloomFilter = null;
        for (int i = 1; i < allBTreeFiles.size(); i++) {
            currentBTree = allBTreeFiles.get(i);
            if (hasBloomFilter) {
                currentBloomFilter = allBloomFilterFiles.get(i);
            }
            // Current start sequence is greater than last stop sequence.
            if (currentBTree.isMoreRecentThan(lastBTree)
                    && (!hasBloomFilter || currentBloomFilter.isMoreRecentThan(lastBloomFilter))) {
                validComparableBTreeFiles.add(currentBTree);
                lastBTree = currentBTree;
                if (hasBloomFilter) {
                    validComparableBloomFilterFiles.add(currentBloomFilter);
                    lastBloomFilter = currentBloomFilter;
                }
            } else if (currentBTree.isWithin(lastBTree)
                    && (!hasBloomFilter || currentBloomFilter.isWithin(lastBloomFilter))) {
                // Invalid files are completely contained in last interval.
                delete(btreeFactory.getBufferCache(), currentBTree.getFileRef());
                if (hasBloomFilter) {
                    delete(btreeFactory.getBufferCache(), currentBloomFilter.getFileRef());
                }
            } else {
                // This scenario should not be possible.
                throw HyracksDataException.create(ErrorCode.FOUND_OVERLAPPING_LSM_FILES, baseDir);
            }
        }

        // Sort valid files in reverse lexicographical order, such that newer
        // files come first.
        Collections.sort(validComparableBTreeFiles, recencyCmp);
        Iterator<IndexComponentFileReference> btreeFileIter = validComparableBTreeFiles.iterator();
        Iterator<IndexComponentFileReference> bloomFilterFileIter = null;
        if (hasBloomFilter) {
            Collections.sort(validComparableBloomFilterFiles, recencyCmp);
            bloomFilterFileIter = validComparableBloomFilterFiles.iterator();
        }
        IndexComponentFileReference cmpBTreeFileName = null;
        IndexComponentFileReference cmpBloomFilterFileName = null;
        while (btreeFileIter.hasNext() && (hasBloomFilter ? bloomFilterFileIter.hasNext() : true)) {
            cmpBTreeFileName = btreeFileIter.next();
            if (hasBloomFilter) {
                cmpBloomFilterFileName = bloomFilterFileIter.next();
            }
            validFiles.add(new LSMComponentFileReferences(cmpBTreeFileName.getFileRef(), null,
                    hasBloomFilter ? cmpBloomFilterFileName.getFileRef() : null));
        }

        return validFiles;
    }

    @Override
    public LSMComponentFileReferences getNewTransactionFileReference() throws IOException {
        String sequence = getNextComponentSequence(btreeFilter);
        // Create transaction lock file
        IoUtil.create(baseDir.getChild(TXN_PREFIX + sequence));
        String baseName = getNextComponentSequence(btreeFilter);
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + BTREE_SUFFIX), null,
                baseDir.getChild(baseName + DELIMITER + BLOOM_FILTER_SUFFIX));
    }

    @Override
    public LSMComponentFileReferences getTransactionFileReferenceForCommit() throws HyracksDataException {
        FilenameFilter transactionFilter;
        String[] files = baseDir.getFile().list(txnFileNameFilter);
        if (files.length == 0) {
            return null;
        }
        if (files.length != 1) {
            throw HyracksDataException.create(ErrorCode.FOUND_MULTIPLE_TRANSACTIONS, baseDir);
        } else {
            transactionFilter = getTransactionFileFilter(true);
            FileReference txnFile = baseDir.getChild(files[0]);
            // get the actual transaction files
            files = baseDir.getFile().list(transactionFilter);
            IoUtil.delete(txnFile);
        }
        FileReference bTreeFileRef = null;
        FileReference bloomFilterFileRef = null;
        for (String fileName : files) {
            if (fileName.endsWith(BTREE_SUFFIX)) {
                bTreeFileRef = baseDir.getChild(fileName);
            } else if (fileName.endsWith(BLOOM_FILTER_SUFFIX)) {
                bloomFilterFileRef = baseDir.getChild(fileName);
            } else {
                throw HyracksDataException.create(ErrorCode.UNRECOGNIZED_INDEX_COMPONENT_FILE, fileName);
            }
        }
        return new LSMComponentFileReferences(bTreeFileRef, null, bloomFilterFileRef);
    }
}
