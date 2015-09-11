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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTreeWithBuddyFileManager extends AbstractLSMIndexFileManager {
    private static final String BUDDY_BTREE_STRING = "buddy";
    private static final String BTREE_STRING = "b";

    private final TreeIndexFactory<? extends ITreeIndex> btreeFactory;
    private final TreeIndexFactory<? extends ITreeIndex> buddyBtreeFactory;

    private static FilenameFilter btreeFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(BTREE_STRING);
        }
    };

    private static FilenameFilter buddyBtreeFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(BUDDY_BTREE_STRING);
        }
    };

    public LSMBTreeWithBuddyFileManager(IFileMapProvider fileMapProvider, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> btreeFactory,
            TreeIndexFactory<? extends ITreeIndex> buddyBtreeFactory) {
        super(fileMapProvider, file, null);
        this.buddyBtreeFactory = buddyBtreeFactory;
        this.btreeFactory = btreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() {
        String ts = getCurrentTimestamp();
        String baseName = baseDir + ts + SPLIT_STRING + ts;
        // Begin timestamp and end timestamp are identical since it is a flush
        return new LSMComponentFileReferences(createFlushFile(baseName + SPLIT_STRING + BTREE_STRING),
                createFlushFile(baseName + SPLIT_STRING + BUDDY_BTREE_STRING), createFlushFile(baseName + SPLIT_STRING
                        + BLOOM_FILTER_STRING));
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName)
            throws HyracksDataException {
        String[] firstTimestampRange = firstFileName.split(SPLIT_STRING);
        String[] lastTimestampRange = lastFileName.split(SPLIT_STRING);

        String baseName = baseDir + firstTimestampRange[0] + SPLIT_STRING + lastTimestampRange[1];
        // Get the range of timestamps by taking the earliest and the latest
        // timestamps
        return new LSMComponentFileReferences(createMergeFile(baseName + SPLIT_STRING + BTREE_STRING),
                createMergeFile(baseName + SPLIT_STRING + BUDDY_BTREE_STRING), createMergeFile(baseName + SPLIT_STRING
                        + BLOOM_FILTER_STRING));
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException, IndexException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<LSMComponentFileReferences>();
        ArrayList<ComparableFileName> allBTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allBuddyBTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allBloomFilterFiles = new ArrayList<ComparableFileName>();

        // Create transaction file filter
        FilenameFilter transactionFilefilter = getTransactionFileFilter(false);

        // Gather files.
        cleanupAndGetValidFilesInternal(getCompoundFilter(btreeFilter, transactionFilefilter), btreeFactory,
                allBTreeFiles);
        HashSet<String> btreeFilesSet = new HashSet<String>();
        for (ComparableFileName cmpFileName : allBTreeFiles) {
            int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
            btreeFilesSet.add(cmpFileName.fileName.substring(0, index));
        }
        validateFiles(btreeFilesSet, allBuddyBTreeFiles, getCompoundFilter(buddyBtreeFilter, transactionFilefilter),
                buddyBtreeFactory);
        validateFiles(btreeFilesSet, allBloomFilterFiles, getCompoundFilter(bloomFilterFilter, transactionFilefilter),
                null);

        // Sanity check.
        if (allBTreeFiles.size() != allBuddyBTreeFiles.size() || allBTreeFiles.size() != allBloomFilterFiles.size()) {
            throw new HyracksDataException(
                    "Unequal number of valid BTree, Buddy BTree, and Bloom Filter files found. Aborting cleanup.");
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

        List<ComparableFileName> validComparableBTreeFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastBTree = allBTreeFiles.get(0);
        validComparableBTreeFiles.add(lastBTree);

        List<ComparableFileName> validComparableBuddyBTreeFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastBuddyBTree = allBuddyBTreeFiles.get(0);
        validComparableBuddyBTreeFiles.add(lastBuddyBTree);

        List<ComparableFileName> validComparableBloomFilterFiles = new ArrayList<ComparableFileName>();
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
                File invalidBTreeFile = new File(currentBTree.fullPath);
                invalidBTreeFile.delete();
                File invalidBuddyBTreeFile = new File(currentBuddyBTree.fullPath);
                invalidBuddyBTreeFile.delete();
                File invalidBloomFilterFile = new File(currentBloomFilter.fullPath);
                invalidBloomFilterFile.delete();
            } else {
                // This scenario should not be possible.
                throw new HyracksDataException("Found LSM files with overlapping but not contained timetamp intervals.");
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
        Files.createFile(Paths.get(baseDir + TRANSACTION_PREFIX + ts));

        String baseName = baseDir + ts + SPLIT_STRING + ts;
        return new LSMComponentFileReferences(createFlushFile(baseName + SPLIT_STRING + BTREE_STRING),
                createFlushFile(baseName + SPLIT_STRING + BUDDY_BTREE_STRING), createFlushFile(baseName + SPLIT_STRING
                        + BLOOM_FILTER_STRING));
    }

    @Override
    public LSMComponentFileReferences getTransactionFileReferenceForCommit() throws HyracksDataException {
        FilenameFilter transactionFilter;
        File dir = new File(baseDir);
        String[] files = dir.list(transactionFileNameFilter);
        if (files.length == 0) {
            return null;
        }
        if (files.length != 1) {
            throw new HyracksDataException("More than one transaction lock found:" + files.length);
        } else {
            transactionFilter = getTransactionFileFilter(true);
            String txnFileName = dir.getPath() + File.separator + files[0];
            // get the actual transaction files
            files = dir.list(transactionFilter);
            if (files.length < 3) {
                throw new HyracksDataException("LSM Btree with buddy transaction has less than 3 files :"
                        + files.length);
            }
            try {
                Files.delete(Paths.get(txnFileName));
            } catch (IOException e) {
                throw new HyracksDataException("Failed to delete transaction lock :" + txnFileName);
            }
        }
        File bTreeFile = null;
        File buddyBTreeFile = null;
        File bloomFilterFile = null;
        for (String fileName : files) {
            if (fileName.endsWith(BTREE_STRING)) {
                bTreeFile = new File(dir.getPath() + File.separator + fileName);
            } else if (fileName.endsWith(BUDDY_BTREE_STRING)) {
                buddyBTreeFile = new File(dir.getPath() + File.separator + fileName);
            } else if (fileName.endsWith(BLOOM_FILTER_STRING)) {
                bloomFilterFile = new File(dir.getPath() + File.separator + fileName);
            } else {
                throw new HyracksDataException("unrecognized file found = " + fileName);
            }
        }
        FileReference bTreeFileRef = new FileReference(bTreeFile);
        FileReference buddyBTreeFileRef = new FileReference(buddyBTreeFile);
        FileReference bloomFilterFileRef = new FileReference(bloomFilterFile);
        return new LSMComponentFileReferences(bTreeFileRef, buddyBTreeFileRef, bloomFilterFileRef);
    }

}
