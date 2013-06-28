/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexFileNameMapper;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

// TODO: Refactor for better code sharing with other file managers.
public class LSMInvertedIndexFileManager extends AbstractLSMIndexFileManager implements IInvertedIndexFileNameMapper {
    private static final String DICT_BTREE_SUFFIX = "b";
    private static final String INVLISTS_SUFFIX = "i";
    private static final String DELETED_KEYS_BTREE_SUFFIX = "d";

    // We only need a BTree factory because the inverted indexes consistency is validated against its dictionary BTree.
    private final BTreeFactory btreeFactory;

    private static FilenameFilter dictBTreeFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(DICT_BTREE_SUFFIX);
        }
    };

    private static FilenameFilter invListFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(INVLISTS_SUFFIX);
        }
    };

    private static FilenameFilter deletedKeysBTreeFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(DELETED_KEYS_BTREE_SUFFIX);
        }
    };

    public LSMInvertedIndexFileManager(IFileMapProvider fileMapProvider, FileReference file, BTreeFactory btreeFactory) {
        super(fileMapProvider, file, null);
        this.btreeFactory = btreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() {
        Date date = new Date();
        String ts = formatter.format(date);
        String baseName = baseDir + ts + SPLIT_STRING + ts;
        // Begin timestamp and end timestamp are identical since it is a flush
        return new LSMComponentFileReferences(createFlushFile(baseName + SPLIT_STRING + DICT_BTREE_SUFFIX),
                createFlushFile(baseName + SPLIT_STRING + DELETED_KEYS_BTREE_SUFFIX), createFlushFile(baseName
                        + SPLIT_STRING + BLOOM_FILTER_STRING));
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName)
            throws HyracksDataException {
        String[] firstTimestampRange = firstFileName.split(SPLIT_STRING);
        String[] lastTimestampRange = lastFileName.split(SPLIT_STRING);

        String baseName = baseDir + firstTimestampRange[0] + SPLIT_STRING + lastTimestampRange[1];
        // Get the range of timestamps by taking the earliest and the latest timestamps
        return new LSMComponentFileReferences(createMergeFile(baseName + SPLIT_STRING + DICT_BTREE_SUFFIX),
                createMergeFile(baseName + SPLIT_STRING + DELETED_KEYS_BTREE_SUFFIX), createMergeFile(baseName
                        + SPLIT_STRING + BLOOM_FILTER_STRING));
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException, IndexException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<LSMComponentFileReferences>();
        ArrayList<ComparableFileName> allDictBTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allInvListsFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allDeletedKeysBTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allBloomFilterFiles = new ArrayList<ComparableFileName>();

        // Gather files.
        cleanupAndGetValidFilesInternal(deletedKeysBTreeFilter, btreeFactory, allDeletedKeysBTreeFiles);
        HashSet<String> deletedKeysBTreeFilesSet = new HashSet<String>();
        for (ComparableFileName cmpFileName : allDeletedKeysBTreeFiles) {
            int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
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
            throw new HyracksDataException(
                    "Unequal number of valid Dictionary BTree, Inverted Lists, Deleted BTree, and Bloom Filter files found. Aborting cleanup.");
        }

        // Trivial cases.
        if (allDictBTreeFiles.isEmpty() || allInvListsFiles.isEmpty() || allDeletedKeysBTreeFiles.isEmpty()
                || allBloomFilterFiles.isEmpty()) {
            return validFiles;
        }

        if (allDictBTreeFiles.size() == 1 && allInvListsFiles.size() == 1 && allDeletedKeysBTreeFiles.size() == 1
                && allBloomFilterFiles.size() == 1) {
            validFiles.add(new LSMComponentFileReferences(allDictBTreeFiles.get(0).fileRef, allDeletedKeysBTreeFiles
                    .get(0).fileRef, allBloomFilterFiles.get(0).fileRef));
            return validFiles;
        }

        // Sorts files names from earliest to latest timestamp.
        Collections.sort(allDeletedKeysBTreeFiles);
        Collections.sort(allDictBTreeFiles);
        Collections.sort(allBloomFilterFiles);

        List<ComparableFileName> validComparableDictBTreeFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastDictBTree = allDictBTreeFiles.get(0);
        validComparableDictBTreeFiles.add(lastDictBTree);

        List<ComparableFileName> validComparableDeletedKeysBTreeFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastDeletedKeysBTree = allDeletedKeysBTreeFiles.get(0);
        validComparableDeletedKeysBTreeFiles.add(lastDeletedKeysBTree);

        List<ComparableFileName> validComparableBloomFilterFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastBloomFilter = allBloomFilterFiles.get(0);
        validComparableBloomFilterFiles.add(lastBloomFilter);

        for (int i = 1; i < allDictBTreeFiles.size(); i++) {
            ComparableFileName currentDeletedKeysBTree = allDeletedKeysBTreeFiles.get(i);
            ComparableFileName CurrentDictBTree = allDictBTreeFiles.get(i);
            ComparableFileName currentBloomFilter = allBloomFilterFiles.get(i);
            // Current start timestamp is greater than last stop timestamp.
            if (currentDeletedKeysBTree.interval[0].compareTo(lastDeletedKeysBTree.interval[1]) > 0
                    && CurrentDictBTree.interval[0].compareTo(lastDictBTree.interval[1]) > 0
                    && currentBloomFilter.interval[0].compareTo(lastBloomFilter.interval[1]) > 0) {
                validComparableDictBTreeFiles.add(CurrentDictBTree);
                validComparableDeletedKeysBTreeFiles.add(currentDeletedKeysBTree);
                validComparableBloomFilterFiles.add(currentBloomFilter);
                lastDictBTree = CurrentDictBTree;
                lastDeletedKeysBTree = currentDeletedKeysBTree;
                lastBloomFilter = currentBloomFilter;
            } else if (currentDeletedKeysBTree.interval[0].compareTo(lastDeletedKeysBTree.interval[0]) >= 0
                    && currentDeletedKeysBTree.interval[1].compareTo(lastDeletedKeysBTree.interval[1]) <= 0
                    && CurrentDictBTree.interval[0].compareTo(lastDictBTree.interval[0]) >= 0
                    && CurrentDictBTree.interval[1].compareTo(lastDictBTree.interval[1]) <= 0
                    && currentBloomFilter.interval[0].compareTo(lastBloomFilter.interval[0]) >= 0
                    && currentBloomFilter.interval[1].compareTo(lastBloomFilter.interval[1]) <= 0) {
                // Invalid files are completely contained in last interval.
                File invalidDeletedBTreeFile = new File(currentDeletedKeysBTree.fullPath);
                invalidDeletedBTreeFile.delete();
                File invalidDictBTreeFile = new File(CurrentDictBTree.fullPath);
                invalidDictBTreeFile.delete();
                File invalidBloomFilterFile = new File(currentBloomFilter.fullPath);
                invalidBloomFilterFile.delete();
            } else {
                // This scenario should not be possible.
                throw new HyracksDataException("Found LSM files with overlapping but not contained timetamp intervals.");
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
        int index = dictBTreeFilePath.lastIndexOf(SPLIT_STRING);
        String file = dictBTreeFilePath.substring(0, index);
        return file + SPLIT_STRING + INVLISTS_SUFFIX;
    }
}
