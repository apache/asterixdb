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

package org.apache.hyracks.storage.am.lsm.rtree.impls;

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

public class LSMRTreeFileManager extends AbstractLSMIndexFileManager {
    public static final String RTREE_STRING = "r";
    public static final String BTREE_STRING = "b";

    private final TreeIndexFactory<? extends ITreeIndex> rtreeFactory;
    private final TreeIndexFactory<? extends ITreeIndex> btreeFactory;

    private static FilenameFilter btreeFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(BTREE_STRING);
        }
    };

    private static FilenameFilter rtreeFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(RTREE_STRING);
        }
    };

    public LSMRTreeFileManager(IFileMapProvider fileMapProvider, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> rtreeFactory, TreeIndexFactory<? extends ITreeIndex> btreeFactory) {
        super(fileMapProvider, file, null);
        this.rtreeFactory = rtreeFactory;
        this.btreeFactory = btreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() {
        String ts = getCurrentTimestamp();
        String baseName = baseDir + ts + SPLIT_STRING + ts;
        // Begin timestamp and end timestamp are identical since it is a flush
        return new LSMComponentFileReferences(createFlushFile(baseName + SPLIT_STRING + RTREE_STRING),
                createFlushFile(baseName + SPLIT_STRING + BTREE_STRING), createFlushFile(baseName + SPLIT_STRING
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
        return new LSMComponentFileReferences(createMergeFile(baseName + SPLIT_STRING + RTREE_STRING),
                createMergeFile(baseName + SPLIT_STRING + BTREE_STRING), createMergeFile(baseName + SPLIT_STRING
                        + BLOOM_FILTER_STRING));
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException, IndexException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<LSMComponentFileReferences>();
        ArrayList<ComparableFileName> allRTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allBTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allBloomFilterFiles = new ArrayList<ComparableFileName>();

        // Create a transaction filter <- to hide transaction components->
        FilenameFilter transactionFilter = getTransactionFileFilter(false);

        // Gather files.
        cleanupAndGetValidFilesInternal(getCompoundFilter(transactionFilter, btreeFilter), btreeFactory, allBTreeFiles);
        HashSet<String> btreeFilesSet = new HashSet<String>();
        for (ComparableFileName cmpFileName : allBTreeFiles) {
            int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
            btreeFilesSet.add(cmpFileName.fileName.substring(0, index));
        }
        validateFiles(btreeFilesSet, allRTreeFiles, getCompoundFilter(transactionFilter, rtreeFilter), rtreeFactory);
        validateFiles(btreeFilesSet, allBloomFilterFiles, getCompoundFilter(transactionFilter, bloomFilterFilter), null);

        // Sanity check.
        if (allRTreeFiles.size() != allBTreeFiles.size() || allBTreeFiles.size() != allBloomFilterFiles.size()) {
            throw new HyracksDataException(
                    "Unequal number of valid RTree, BTree, and Bloom Filter files found. Aborting cleanup.");
        }

        // Trivial cases.
        if (allRTreeFiles.isEmpty() || allBTreeFiles.isEmpty() || allBloomFilterFiles.isEmpty()) {
            return validFiles;
        }

        if (allRTreeFiles.size() == 1 && allBTreeFiles.size() == 1 && allBloomFilterFiles.size() == 1) {
            validFiles.add(new LSMComponentFileReferences(allRTreeFiles.get(0).fileRef, allBTreeFiles.get(0).fileRef,
                    allBloomFilterFiles.get(0).fileRef));
            return validFiles;
        }

        // Sorts files names from earliest to latest timestamp.
        Collections.sort(allRTreeFiles);
        Collections.sort(allBTreeFiles);
        Collections.sort(allBloomFilterFiles);

        List<ComparableFileName> validComparableRTreeFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastRTree = allRTreeFiles.get(0);
        validComparableRTreeFiles.add(lastRTree);

        List<ComparableFileName> validComparableBTreeFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastBTree = allBTreeFiles.get(0);
        validComparableBTreeFiles.add(lastBTree);

        List<ComparableFileName> validComparableBloomFilterFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastBloomFilter = allBloomFilterFiles.get(0);
        validComparableBloomFilterFiles.add(lastBloomFilter);

        for (int i = 1; i < allRTreeFiles.size(); i++) {
            ComparableFileName currentRTree = allRTreeFiles.get(i);
            ComparableFileName currentBTree = allBTreeFiles.get(i);
            ComparableFileName currentBloomFilter = allBloomFilterFiles.get(i);
            // Current start timestamp is greater than last stop timestamp.
            if (currentRTree.interval[0].compareTo(lastRTree.interval[1]) > 0
                    && currentBTree.interval[0].compareTo(lastBTree.interval[1]) > 0
                    && currentBloomFilter.interval[0].compareTo(lastBloomFilter.interval[1]) > 0) {
                validComparableRTreeFiles.add(currentRTree);
                validComparableBTreeFiles.add(currentBTree);
                validComparableBloomFilterFiles.add(currentBloomFilter);
                lastRTree = currentRTree;
                lastBTree = currentBTree;
                lastBloomFilter = currentBloomFilter;
            } else if (currentRTree.interval[0].compareTo(lastRTree.interval[0]) >= 0
                    && currentRTree.interval[1].compareTo(lastRTree.interval[1]) <= 0
                    && currentBTree.interval[0].compareTo(lastBTree.interval[0]) >= 0
                    && currentBTree.interval[1].compareTo(lastBTree.interval[1]) <= 0
                    && currentBloomFilter.interval[0].compareTo(lastBloomFilter.interval[0]) >= 0
                    && currentBloomFilter.interval[1].compareTo(lastBloomFilter.interval[1]) <= 0) {
                // Invalid files are completely contained in last interval.
                File invalidRTreeFile = new File(currentRTree.fullPath);
                invalidRTreeFile.delete();
                File invalidBTreeFile = new File(currentBTree.fullPath);
                invalidBTreeFile.delete();
                File invalidBloomFilterFile = new File(currentBloomFilter.fullPath);
                invalidBloomFilterFile.delete();
            } else {
                // This scenario should not be possible.
                throw new HyracksDataException("Found LSM files with overlapping but not contained timetamp intervals.");
            }
        }

        // Sort valid files in reverse lexicographical order, such that newer
        // files come first.
        Collections.sort(validComparableRTreeFiles, recencyCmp);
        Collections.sort(validComparableBTreeFiles, recencyCmp);
        Collections.sort(validComparableBloomFilterFiles, recencyCmp);

        Iterator<ComparableFileName> rtreeFileIter = validComparableRTreeFiles.iterator();
        Iterator<ComparableFileName> btreeFileIter = validComparableBTreeFiles.iterator();
        Iterator<ComparableFileName> bloomFilterFileIter = validComparableBloomFilterFiles.iterator();
        while (rtreeFileIter.hasNext() && btreeFileIter.hasNext()) {
            ComparableFileName cmpRTreeFileName = rtreeFileIter.next();
            ComparableFileName cmpBTreeFileName = btreeFileIter.next();
            ComparableFileName cmpBloomFilterFileName = bloomFilterFileIter.next();
            validFiles.add(new LSMComponentFileReferences(cmpRTreeFileName.fileRef, cmpBTreeFileName.fileRef,
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
        return new LSMComponentFileReferences(createFlushFile(baseName + SPLIT_STRING + RTREE_STRING),
                createFlushFile(baseName + SPLIT_STRING + BTREE_STRING), createFlushFile(baseName + SPLIT_STRING
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
                throw new HyracksDataException("LSM Rtree transaction has less than 3 files :" + files.length);
            }
            try {
                Files.delete(Paths.get(txnFileName));
            } catch (IOException e) {
                throw new HyracksDataException("Failed to delete transaction lock :" + txnFileName);
            }
        }
        File rTreeFile = null;
        File bTreeFile = null;
        File bloomFilterFile = null;
        for (String fileName : files) {
            if (fileName.endsWith(BTREE_STRING)) {
                bTreeFile = new File(dir.getPath() + File.separator + fileName);
            } else if (fileName.endsWith(RTREE_STRING)) {
                rTreeFile = new File(dir.getPath() + File.separator + fileName);
            } else if (fileName.endsWith(BLOOM_FILTER_STRING)) {
                bloomFilterFile = new File(dir.getPath() + File.separator + fileName);
            } else {
                throw new HyracksDataException("unrecognized file found = " + fileName);
            }
        }
        FileReference rTreeFileRef = new FileReference(rTreeFile);
        FileReference bTreeFileRef = new FileReference(bTreeFile);
        FileReference bloomFilterFileRef = new FileReference(bloomFilterFile);
        return new LSMComponentFileReferences(rTreeFileRef, bTreeFileRef, bloomFilterFileRef);
    }
}
