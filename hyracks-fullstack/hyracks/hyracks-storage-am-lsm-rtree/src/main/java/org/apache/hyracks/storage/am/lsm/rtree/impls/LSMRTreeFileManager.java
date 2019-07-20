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
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;

public class LSMRTreeFileManager extends AbstractLSMIndexFileManager {

    private final TreeIndexFactory<? extends ITreeIndex> rtreeFactory;
    private final TreeIndexFactory<? extends ITreeIndex> btreeFactory;

    private static final FilenameFilter btreeFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(BTREE_SUFFIX);
    private static final FilenameFilter rtreeFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(RTREE_SUFFIX);

    public LSMRTreeFileManager(IIOManager ioManager, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> rtreeFactory, TreeIndexFactory<? extends ITreeIndex> btreeFactory) {
        super(ioManager, file, null);
        this.rtreeFactory = rtreeFactory;
        this.btreeFactory = btreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() throws HyracksDataException {
        String baseName = getNextComponentSequence(btreeFilter);
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + RTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BLOOM_FILTER_SUFFIX));
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName) {
        final String baseName = IndexComponentFileReference.getMergeSequence(firstFileName, lastFileName);
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + RTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BLOOM_FILTER_SUFFIX));
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<>();
        ArrayList<IndexComponentFileReference> allRTreeFiles = new ArrayList<>();
        ArrayList<IndexComponentFileReference> allBTreeFiles = new ArrayList<>();
        ArrayList<IndexComponentFileReference> allBloomFilterFiles = new ArrayList<>();

        // Create a transaction filter <- to hide transaction components->
        FilenameFilter transactionFilter = getTransactionFileFilter(false);

        // Gather files.
        cleanupAndGetValidFilesInternal(getCompoundFilter(transactionFilter, btreeFilter), btreeFactory, allBTreeFiles,
                btreeFactory.getBufferCache());
        HashSet<String> btreeFilesSet = new HashSet<>();
        for (IndexComponentFileReference cmpFileName : allBTreeFiles) {
            btreeFilesSet.add(cmpFileName.getSequence());
        }
        validateFiles(btreeFilesSet, allRTreeFiles, getCompoundFilter(transactionFilter, rtreeFilter), rtreeFactory,
                btreeFactory.getBufferCache());
        validateFiles(btreeFilesSet, allBloomFilterFiles, getCompoundFilter(transactionFilter, bloomFilterFilter), null,
                btreeFactory.getBufferCache());

        // Sanity check.
        if (allRTreeFiles.size() != allBTreeFiles.size() || allBTreeFiles.size() != allBloomFilterFiles.size()) {
            throw HyracksDataException.create(ErrorCode.UNEQUAL_NUM_FILTERS_TREES, baseDir);
        }

        // Trivial cases.
        if (allRTreeFiles.isEmpty() || allBTreeFiles.isEmpty() || allBloomFilterFiles.isEmpty()) {
            return validFiles;
        }

        if (allRTreeFiles.size() == 1 && allBTreeFiles.size() == 1 && allBloomFilterFiles.size() == 1) {
            validFiles.add(new LSMComponentFileReferences(allRTreeFiles.get(0).getFileRef(),
                    allBTreeFiles.get(0).getFileRef(), allBloomFilterFiles.get(0).getFileRef()));
            return validFiles;
        }

        // Sorts files names from earliest to latest sequence.
        Collections.sort(allRTreeFiles);
        Collections.sort(allBTreeFiles);
        Collections.sort(allBloomFilterFiles);

        List<IndexComponentFileReference> validComparableRTreeFiles = new ArrayList<>();
        IndexComponentFileReference lastRTree = allRTreeFiles.get(0);
        validComparableRTreeFiles.add(lastRTree);

        List<IndexComponentFileReference> validComparableBTreeFiles = new ArrayList<>();
        IndexComponentFileReference lastBTree = allBTreeFiles.get(0);
        validComparableBTreeFiles.add(lastBTree);

        List<IndexComponentFileReference> validComparableBloomFilterFiles = new ArrayList<>();
        IndexComponentFileReference lastBloomFilter = allBloomFilterFiles.get(0);
        validComparableBloomFilterFiles.add(lastBloomFilter);

        for (int i = 1; i < allRTreeFiles.size(); i++) {
            IndexComponentFileReference currentRTree = allRTreeFiles.get(i);
            IndexComponentFileReference currentBTree = allBTreeFiles.get(i);
            IndexComponentFileReference currentBloomFilter = allBloomFilterFiles.get(i);
            // Current start sequence is greater than last stop sequence.
            if (currentRTree.isMoreRecentThan(lastRTree) && currentBTree.isMoreRecentThan(lastBTree)
                    && currentBloomFilter.isMoreRecentThan(lastBloomFilter)) {
                validComparableRTreeFiles.add(currentRTree);
                validComparableBTreeFiles.add(currentBTree);
                validComparableBloomFilterFiles.add(currentBloomFilter);
                lastRTree = currentRTree;
                lastBTree = currentBTree;
                lastBloomFilter = currentBloomFilter;
            } else if (currentRTree.isWithin(lastRTree) && currentBTree.isWithin(lastBTree)
                    && currentBloomFilter.isWithin(lastBloomFilter)) {
                // Invalid files are completely contained in last sequence.
                delete(treeFactory.getBufferCache(), currentRTree.getFileRef());
                delete(treeFactory.getBufferCache(), currentBTree.getFileRef());
                delete(treeFactory.getBufferCache(), currentBloomFilter.getFileRef());
            } else {
                // This scenario should not be possible.
                throw HyracksDataException.create(ErrorCode.FOUND_OVERLAPPING_LSM_FILES, baseDir);
            }
        }

        // Sort valid files in reverse lexicographical order, such that newer
        // files come first.
        validComparableRTreeFiles.sort(recencyCmp);
        validComparableBTreeFiles.sort(recencyCmp);
        validComparableBloomFilterFiles.sort(recencyCmp);

        Iterator<IndexComponentFileReference> rtreeFileIter = validComparableRTreeFiles.iterator();
        Iterator<IndexComponentFileReference> btreeFileIter = validComparableBTreeFiles.iterator();
        Iterator<IndexComponentFileReference> bloomFilterFileIter = validComparableBloomFilterFiles.iterator();
        while (rtreeFileIter.hasNext() && btreeFileIter.hasNext()) {
            IndexComponentFileReference cmpRTreeFileName = rtreeFileIter.next();
            IndexComponentFileReference cmpBTreeFileName = btreeFileIter.next();
            IndexComponentFileReference cmpBloomFilterFileName = bloomFilterFileIter.next();
            validFiles.add(new LSMComponentFileReferences(cmpRTreeFileName.getFileRef(), cmpBTreeFileName.getFileRef(),
                    cmpBloomFilterFileName.getFileRef()));
        }
        return validFiles;
    }

    @Override
    public LSMComponentFileReferences getNewTransactionFileReference() throws IOException {
        String baseName = getNextComponentSequence(btreeFilter);
        // Create transaction lock file
        Files.createFile(Paths.get(baseDir + TXN_PREFIX + baseName));
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + RTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BTREE_SUFFIX),
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
        FileReference rTreeFileRef = null;
        FileReference bTreeFileRef = null;
        FileReference bloomFilterFileRef = null;
        for (String fileName : files) {
            if (fileName.endsWith(BTREE_SUFFIX)) {
                bTreeFileRef = baseDir.getChild(fileName);
            } else if (fileName.endsWith(RTREE_SUFFIX)) {
                rTreeFileRef = baseDir.getChild(fileName);
            } else if (fileName.endsWith(BLOOM_FILTER_SUFFIX)) {
                bloomFilterFileRef = baseDir.getChild(fileName);
            } else {
                throw HyracksDataException.create(ErrorCode.UNRECOGNIZED_INDEX_COMPONENT_FILE, fileName);
            }
        }
        return new LSMComponentFileReferences(rTreeFileRef, bTreeFileRef, bloomFilterFileRef);
    }
}
