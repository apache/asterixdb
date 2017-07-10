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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.io.File;
import java.io.FilenameFilter;
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
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexFileNameMapper;

// TODO: Refactor for better code sharing with other file managers.
public class LSMInvertedIndexFileManager extends AbstractLSMIndexFileManager implements IInvertedIndexFileNameMapper {
    public static final String DICT_BTREE_SUFFIX = "b";
    public static final String INVLISTS_SUFFIX = "i";
    public static final String DELETED_KEYS_BTREE_SUFFIX = "d";

    // We only need a BTree factory because the inverted indexes consistency is validated against its dictionary BTree.
    private final BTreeFactory btreeFactory;
    private static final FilenameFilter dictBTreeFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(DICT_BTREE_SUFFIX);
    private static final FilenameFilter invListFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(INVLISTS_SUFFIX);
    private static final FilenameFilter deletedKeysBTreeFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(DELETED_KEYS_BTREE_SUFFIX);

    public LSMInvertedIndexFileManager(IIOManager ioManager, FileReference file, BTreeFactory btreeFactory) {
        super(ioManager, file, null);
        this.btreeFactory = btreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() throws HyracksDataException {
        String ts = getCurrentTimestamp();
        String baseName = ts + DELIMITER + ts;
        // Begin timestamp and end timestamp are identical since it is a flush
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + DICT_BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + DELETED_KEYS_BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BLOOM_FILTER_SUFFIX));
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName)
            throws HyracksDataException {
        String[] firstTimestampRange = firstFileName.split(DELIMITER);
        String[] lastTimestampRange = lastFileName.split(DELIMITER);
        String baseName = firstTimestampRange[0] + DELIMITER + lastTimestampRange[1];
        // Get the range of timestamps by taking the earliest and the latest timestamps
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + DICT_BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + DELETED_KEYS_BTREE_SUFFIX),
                baseDir.getChild(baseName + DELIMITER + BLOOM_FILTER_SUFFIX));
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<>();
        ArrayList<ComparableFileName> allDictBTreeFiles = new ArrayList<>();
        ArrayList<ComparableFileName> allInvListsFiles = new ArrayList<>();
        ArrayList<ComparableFileName> allDeletedKeysBTreeFiles = new ArrayList<>();
        ArrayList<ComparableFileName> allBloomFilterFiles = new ArrayList<>();

        // Gather files.
        cleanupAndGetValidFilesInternal(deletedKeysBTreeFilter, btreeFactory, allDeletedKeysBTreeFiles);
        HashSet<String> deletedKeysBTreeFilesSet = new HashSet<>();
        for (ComparableFileName cmpFileName : allDeletedKeysBTreeFiles) {
            int index = cmpFileName.fileName.lastIndexOf(DELIMITER);
            deletedKeysBTreeFilesSet.add(cmpFileName.fileName.substring(0, index));
        }

        // TODO: do we really need to validate the inverted lists files or is validating the dict. BTrees is enough?
        validateFiles(deletedKeysBTreeFilesSet, allInvListsFiles, invListFilter, null);
        validateFiles(deletedKeysBTreeFilesSet, allDictBTreeFiles, dictBTreeFilter, btreeFactory);
        validateFiles(deletedKeysBTreeFilesSet, allBloomFilterFiles, bloomFilterFilter, null);

        // Sanity check.
        if (allDictBTreeFiles.size() != allInvListsFiles.size()
                || allDictBTreeFiles.size() != allDeletedKeysBTreeFiles.size()
                || allDictBTreeFiles.size() != allBloomFilterFiles.size()) {
            throw HyracksDataException.create(ErrorCode.UNEQUAL_NUM_FILTERS_TREES, baseDir);
        }

        // Trivial cases.
        if (allDictBTreeFiles.isEmpty() || allInvListsFiles.isEmpty() || allDeletedKeysBTreeFiles.isEmpty()
                || allBloomFilterFiles.isEmpty()) {
            return validFiles;
        }

        if (allDictBTreeFiles.size() == 1 && allInvListsFiles.size() == 1 && allDeletedKeysBTreeFiles.size() == 1
                && allBloomFilterFiles.size() == 1) {
            validFiles.add(new LSMComponentFileReferences(allDictBTreeFiles.get(0).fileRef,
                    allDeletedKeysBTreeFiles.get(0).fileRef, allBloomFilterFiles.get(0).fileRef));
            return validFiles;
        }

        // Sorts files names from earliest to latest timestamp.
        Collections.sort(allDeletedKeysBTreeFiles);
        Collections.sort(allDictBTreeFiles);
        Collections.sort(allBloomFilterFiles);

        List<ComparableFileName> validComparableDictBTreeFiles = new ArrayList<>();
        ComparableFileName lastDictBTree = allDictBTreeFiles.get(0);
        validComparableDictBTreeFiles.add(lastDictBTree);

        List<ComparableFileName> validComparableDeletedKeysBTreeFiles = new ArrayList<>();
        ComparableFileName lastDeletedKeysBTree = allDeletedKeysBTreeFiles.get(0);
        validComparableDeletedKeysBTreeFiles.add(lastDeletedKeysBTree);

        List<ComparableFileName> validComparableBloomFilterFiles = new ArrayList<>();
        ComparableFileName lastBloomFilter = allBloomFilterFiles.get(0);
        validComparableBloomFilterFiles.add(lastBloomFilter);

        for (int i = 1; i < allDictBTreeFiles.size(); i++) {
            ComparableFileName currentDeletedKeysBTree = allDeletedKeysBTreeFiles.get(i);
            ComparableFileName currentDictBTree = allDictBTreeFiles.get(i);
            ComparableFileName currentBloomFilter = allBloomFilterFiles.get(i);
            // Current start timestamp is greater than last stop timestamp.
            if (currentDeletedKeysBTree.interval[0].compareTo(lastDeletedKeysBTree.interval[1]) > 0
                    && currentDictBTree.interval[0].compareTo(lastDictBTree.interval[1]) > 0
                    && currentBloomFilter.interval[0].compareTo(lastBloomFilter.interval[1]) > 0) {
                validComparableDictBTreeFiles.add(currentDictBTree);
                validComparableDeletedKeysBTreeFiles.add(currentDeletedKeysBTree);
                validComparableBloomFilterFiles.add(currentBloomFilter);
                lastDictBTree = currentDictBTree;
                lastDeletedKeysBTree = currentDeletedKeysBTree;
                lastBloomFilter = currentBloomFilter;
            } else if (currentDeletedKeysBTree.interval[0].compareTo(lastDeletedKeysBTree.interval[0]) >= 0
                    && currentDeletedKeysBTree.interval[1].compareTo(lastDeletedKeysBTree.interval[1]) <= 0
                    && currentDictBTree.interval[0].compareTo(lastDictBTree.interval[0]) >= 0
                    && currentDictBTree.interval[1].compareTo(lastDictBTree.interval[1]) <= 0
                    && currentBloomFilter.interval[0].compareTo(lastBloomFilter.interval[0]) >= 0
                    && currentBloomFilter.interval[1].compareTo(lastBloomFilter.interval[1]) <= 0) {
                // Invalid files are completely contained in last interval.
                IoUtil.delete(new File(currentDeletedKeysBTree.fullPath));
                IoUtil.delete(new File(currentDictBTree.fullPath));
                IoUtil.delete(new File(currentBloomFilter.fullPath));
            } else {
                // This scenario should not be possible.
                throw HyracksDataException.create(ErrorCode.FOUND_OVERLAPPING_LSM_FILES, baseDir);
            }
        }

        // Sort valid files in reverse lexicographical order, such that newer
        // files come first.
        Collections.sort(validComparableDictBTreeFiles, recencyCmp);
        Collections.sort(validComparableDeletedKeysBTreeFiles, recencyCmp);
        Collections.sort(validComparableBloomFilterFiles, recencyCmp);

        Iterator<ComparableFileName> dictBTreeFileIter = validComparableDictBTreeFiles.iterator();
        Iterator<ComparableFileName> deletedKeysBTreeIter = validComparableDeletedKeysBTreeFiles.iterator();
        Iterator<ComparableFileName> bloomFilterFileIter = validComparableBloomFilterFiles.iterator();
        while (dictBTreeFileIter.hasNext() && deletedKeysBTreeIter.hasNext()) {
            ComparableFileName cmpDictBTreeFile = dictBTreeFileIter.next();
            ComparableFileName cmpDeletedKeysBTreeFile = deletedKeysBTreeIter.next();
            ComparableFileName cmpBloomFilterFileName = bloomFilterFileIter.next();
            validFiles.add(new LSMComponentFileReferences(cmpDictBTreeFile.fileRef, cmpDeletedKeysBTreeFile.fileRef,
                    cmpBloomFilterFileName.fileRef));
        }

        return validFiles;
    }

    @Override
    public String getInvListsFilePath(String dictBTreeFilePath) {
        int index = dictBTreeFilePath.lastIndexOf(DELIMITER);
        String file = dictBTreeFilePath.substring(0, index);
        return file + DELIMITER + INVLISTS_SUFFIX;
    }
}
