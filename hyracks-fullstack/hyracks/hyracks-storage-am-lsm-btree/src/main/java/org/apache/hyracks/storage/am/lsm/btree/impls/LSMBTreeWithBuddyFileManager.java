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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;

public class LSMBTreeWithBuddyFileManager extends AbstractLSMIndexFileManager {

    private final TreeIndexFactory<? extends ITreeIndex> btreeFactory;
    private final TreeIndexFactory<? extends ITreeIndex> buddyBtreeFactory;

    private static FilenameFilter btreeFilter = (dir, name) -> !name.startsWith(".") && name.endsWith(BTREE_SUFFIX);

    private static FilenameFilter buddyBtreeFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(DELETE_TREE_SUFFIX);

    public LSMBTreeWithBuddyFileManager(IIOManager ioManager, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> btreeFactory,
            TreeIndexFactory<? extends ITreeIndex> buddyBtreeFactory) {
        super(ioManager, file, null);
        this.buddyBtreeFactory = buddyBtreeFactory;
        this.btreeFactory = btreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() throws HyracksDataException {
        String ts = getCurrentTimestamp();
        String baseName = ts + DELIMITER + ts;
        // Begin timestamp and end timestamp are identical since it is a flush
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + DELETE_TREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BLOOM_FILTER_SUFFIX));
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName)
            throws HyracksDataException {
        String[] firstTimestampRange = firstFileName.split(DELIMITER);
        String[] lastTimestampRange = lastFileName.split(DELIMITER);

        String baseName = firstTimestampRange[0] + DELIMITER + lastTimestampRange[1];
        // Get the range of timestamps by taking the earliest and the latest
        // timestamps
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + DELETE_TREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BLOOM_FILTER_SUFFIX));
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<>();
        ArrayList<ComparableFileName> allBTreeFiles = new ArrayList<>();
        ArrayList<ComparableFileName> allBuddyBTreeFiles = new ArrayList<>();
        ArrayList<ComparableFileName> allBloomFilterFiles = new ArrayList<>();
        // Create transaction file filter
        FilenameFilter transactionFilefilter = getTransactionFileFilter(false);
        // Gather files.
        cleanupAndGetValidFilesInternal(getCompoundFilter(btreeFilter, transactionFilefilter), btreeFactory,
                allBTreeFiles);
        HashSet<String> btreeFilesSet = new HashSet<>();
        for (ComparableFileName cmpFileName : allBTreeFiles) {
            int index = cmpFileName.fileName.lastIndexOf(DELIMITER);
            btreeFilesSet.add(cmpFileName.fileName.substring(0, index));
        }
        validateFiles(btreeFilesSet, allBuddyBTreeFiles, getCompoundFilter(buddyBtreeFilter, transactionFilefilter),
                buddyBtreeFactory);
        validateFiles(btreeFilesSet, allBloomFilterFiles, getCompoundFilter(bloomFilterFilter, transactionFilefilter),
                null);
        // Sanity check.
        if (allBTreeFiles.size() != allBuddyBTreeFiles.size() || allBTreeFiles.size() != allBloomFilterFiles.size()) {
            throw HyracksDataException.create(ErrorCode.UNEQUAL_NUM_FILTERS_TREES, baseDir);
        }
        // Trivial cases.
        if (allBTreeFiles.isEmpty() || allBuddyBTreeFiles.isEmpty() || allBloomFilterFiles.isEmpty()) {
            return validFiles;
        }
        if (allBTreeFiles.size() == 1 && allBuddyBTreeFiles.size() == 1 && allBloomFilterFiles.size() == 1) {
            validFiles.add(new LSMComponentFileReferences(allBTreeFiles.get(0).fileRef,
                    allBuddyBTreeFiles.get(0).fileRef, allBloomFilterFiles.get(0).fileRef));
            return validFiles;
        }

        // Sorts files names from earliest to latest timestamp.
        Collections.sort(allBTreeFiles);
        Collections.sort(allBuddyBTreeFiles);
        Collections.sort(allBloomFilterFiles);

        List<ComparableFileName> validComparableBTreeFiles = new ArrayList<>();
        ComparableFileName lastBTree = allBTreeFiles.get(0);
        validComparableBTreeFiles.add(lastBTree);

        List<ComparableFileName> validComparableBuddyBTreeFiles = new ArrayList<>();
        ComparableFileName lastBuddyBTree = allBuddyBTreeFiles.get(0);
        validComparableBuddyBTreeFiles.add(lastBuddyBTree);

        List<ComparableFileName> validComparableBloomFilterFiles = new ArrayList<>();
        ComparableFileName lastBloomFilter = allBloomFilterFiles.get(0);
        validComparableBloomFilterFiles.add(lastBloomFilter);

        for (int i = 1; i < allBTreeFiles.size(); i++) {
            ComparableFileName currentBTree = allBTreeFiles.get(i);
            ComparableFileName currentBuddyBTree = allBuddyBTreeFiles.get(i);
            ComparableFileName currentBloomFilter = allBloomFilterFiles.get(i);
            // Current start timestamp is greater than last stop timestamp.
            if (currentBTree.interval[0].compareTo(lastBTree.interval[1]) > 0
                    && currentBuddyBTree.interval[0].compareTo(lastBuddyBTree.interval[1]) > 0
                    && currentBloomFilter.interval[0].compareTo(lastBloomFilter.interval[1]) > 0) {
                validComparableBTreeFiles.add(currentBTree);
                validComparableBuddyBTreeFiles.add(currentBuddyBTree);
                validComparableBloomFilterFiles.add(currentBloomFilter);
                lastBTree = currentBTree;
                lastBuddyBTree = currentBuddyBTree;
                lastBloomFilter = currentBloomFilter;
            } else if (currentBTree.interval[0].compareTo(lastBTree.interval[0]) >= 0
                    && currentBTree.interval[1].compareTo(lastBTree.interval[1]) <= 0
                    && currentBuddyBTree.interval[0].compareTo(lastBuddyBTree.interval[0]) >= 0
                    && currentBuddyBTree.interval[1].compareTo(lastBuddyBTree.interval[1]) <= 0
                    && currentBloomFilter.interval[0].compareTo(lastBloomFilter.interval[0]) >= 0
                    && currentBloomFilter.interval[1].compareTo(lastBloomFilter.interval[1]) <= 0) {
                // Invalid files are completely contained in last interval.
                IoUtil.delete(new File(currentBTree.fullPath));
                IoUtil.delete(new File(currentBuddyBTree.fullPath));
                IoUtil.delete(new File(currentBloomFilter.fullPath));
            } else {
                // This scenario should not be possible.
                throw HyracksDataException.create(ErrorCode.FOUND_OVERLAPPING_LSM_FILES, baseDir);
            }
        }

        // Sort valid files in reverse lexicographical order, such that newer
        // files come first.
        Collections.sort(validComparableBTreeFiles, recencyCmp);
        Collections.sort(validComparableBuddyBTreeFiles, recencyCmp);
        Collections.sort(validComparableBloomFilterFiles, recencyCmp);

        Iterator<ComparableFileName> btreeFileIter = validComparableBTreeFiles.iterator();
        Iterator<ComparableFileName> buddyBtreeFileIter = validComparableBuddyBTreeFiles.iterator();
        Iterator<ComparableFileName> bloomFilterFileIter = validComparableBloomFilterFiles.iterator();
        while (btreeFileIter.hasNext() && buddyBtreeFileIter.hasNext()) {
            ComparableFileName cmpBTreeFileName = btreeFileIter.next();
            ComparableFileName cmpBuddyBTreeFileName = buddyBtreeFileIter.next();
            ComparableFileName cmpBloomFilterFileName = bloomFilterFileIter.next();
            validFiles.add(new LSMComponentFileReferences(cmpBTreeFileName.fileRef, cmpBuddyBTreeFileName.fileRef,
                    cmpBloomFilterFileName.fileRef));
        }

        return validFiles;
    }

    @Override
    public LSMComponentFileReferences getNewTransactionFileReference() throws IOException {
        String ts = getCurrentTimestamp();
        // Create transaction lock file
        Files.createFile(Paths.get(baseDir + TXN_PREFIX + ts));
        String baseName = ts + DELIMITER + ts;
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + DELETE_TREE_SUFFIX),
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
            // get the actual transaction files
            files = baseDir.getFile().list(transactionFilter);
            if (files.length < 3) {
                throw HyracksDataException.create(ErrorCode.UNEQUAL_NUM_FILTERS_TREES, baseDir);
            }
            IoUtil.delete(baseDir.getChild(files[0]));
        }
        FileReference bTreeFileRef = null;
        FileReference buddyBTreeFileRef = null;
        FileReference bloomFilterFileRef = null;
        for (String fileName : files) {
            if (fileName.endsWith(BTREE_SUFFIX)) {
                bTreeFileRef = baseDir.getChild(fileName);
            } else if (fileName.endsWith(DELETE_TREE_SUFFIX)) {
                buddyBTreeFileRef = baseDir.getChild(fileName);
            } else if (fileName.endsWith(BLOOM_FILTER_SUFFIX)) {
                bloomFilterFileRef = baseDir.getChild(fileName);
            } else {
                throw HyracksDataException.create(ErrorCode.UNRECOGNIZED_INDEX_COMPONENT_FILE, fileName);
            }
        }

        return new LSMComponentFileReferences(bTreeFileRef, buddyBTreeFileRef, bloomFilterFileRef);
    }

}
