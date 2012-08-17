/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

// TODO: Implement this one properly!
public class LSMInvertedIndexFileManager extends LSMIndexFileManager {
    private static final String BTREE_SUFFIX = "b";
    private static final String INVLISTS_SUFFIX = "i";
    private static final String DELETED_KEYS_BTREE_SUFFIX = "d";

    private final TreeIndexFactory<? extends ITreeIndex> rtreeFactory;
    private final TreeIndexFactory<? extends ITreeIndex> btreeFactory;

    private static FilenameFilter btreeFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(BTREE_SUFFIX);
        }
    };

    private static FilenameFilter deletedKeysBTreeFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(DELETED_KEYS_BTREE_SUFFIX);
        }
    };

    public LSMInvertedIndexFileManager(IIOManager ioManager, IFileMapProvider fileMapProvider, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> rtreeFactory, TreeIndexFactory<? extends ITreeIndex> btreeFactory) {
        super(ioManager, fileMapProvider, file, null);
        this.rtreeFactory = rtreeFactory;
        this.btreeFactory = btreeFactory;
    }

    @Override
    public Object getRelFlushFileName() {
        String baseName = (String) super.getRelFlushFileName();
        return new LSMInvertedIndexFileNameComponent(baseName + SPLIT_STRING + BTREE_SUFFIX, baseName + SPLIT_STRING
                + INVLISTS_SUFFIX, baseName + DELETED_KEYS_BTREE_SUFFIX);

    }

    @Override
    public Object getRelMergeFileName(String firstFileName, String lastFileName) throws HyracksDataException {
        String baseName = (String) super.getRelMergeFileName(firstFileName, lastFileName);
        return new LSMInvertedIndexFileNameComponent(baseName + SPLIT_STRING + BTREE_SUFFIX, baseName + SPLIT_STRING
                + INVLISTS_SUFFIX, baseName + DELETED_KEYS_BTREE_SUFFIX);
    }

    @Override
    public List<Object> cleanupAndGetValidFiles(ILSMComponentFinalizer componentFinalizer) throws HyracksDataException {
        List<Object> validFiles = new ArrayList<Object>();
        ArrayList<ComparableFileName> allBTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allInvListsFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allDeletedKeysBTreeFiles = new ArrayList<ComparableFileName>();

        // Gather files from all IODeviceHandles.
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            cleanupAndGetValidFilesInternal(dev, deletedKeysBTreeFilter, btreeFactory, componentFinalizer,
                    allDeletedKeysBTreeFiles);
            HashSet<String> deletedKeysBTreeFilesSet = new HashSet<String>();
            for (ComparableFileName cmpFileName : allBTreeFiles) {
                int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
                deletedKeysBTreeFilesSet.add(cmpFileName.fileName.substring(0, index));
            }
            // We use the dictionary BTree of the inverted index for validation.
            // List of valid dictionary BTree files that may or may not have a deleted-keys BTree buddy. Will check for buddies below.
            ArrayList<ComparableFileName> tmpAllBTreeFiles = new ArrayList<ComparableFileName>();
            // TODO: Fix factory.
            cleanupAndGetValidFilesInternal(dev, btreeFilter, rtreeFactory, componentFinalizer, tmpAllBTreeFiles);
            // Look for buddy deleted-keys BTrees for all valid dictionary BTrees. 
            // If no buddy is found, delete the file, otherwise add the dictionary BTree to allBTreeFiles. 
            for (ComparableFileName cmpFileName : tmpAllBTreeFiles) {
                int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
                String file = cmpFileName.fileName.substring(0, index);
                if (deletedKeysBTreeFilesSet.contains(file)) {
                    allBTreeFiles.add(cmpFileName);
                } else {
                    // Couldn't find the corresponding BTree file; thus, delete
                    // the deleted-keys BTree file.
                    // There is no need to delete the inverted-lists file corresponding to the non-existent
                    // dictionary BTree, because we flush the dictionary BTree first. So if it a dictionary BTree 
                    // file does not exists, then neither can its inverted-list file.
                    File invalidDeletedKeysBTreeFile = new File(cmpFileName.fullPath);
                    invalidDeletedKeysBTreeFile.delete();
                }
            }
        }
        // Sanity check.
        if (allBTreeFiles.size() != allDeletedKeysBTreeFiles.size()) {
            throw new HyracksDataException("Unequal number of valid RTree and BTree files found. Aborting cleanup.");
        }

        // Trivial cases.
        if (allBTreeFiles.isEmpty() || allDeletedKeysBTreeFiles.isEmpty()) {
            return validFiles;
        }

        // TODO: Continue from here.
        if (allBTreeFiles.size() == 1 && allDeletedKeysBTreeFiles.size() == 1) {
            validFiles.add(new LSMInvertedIndexFileNameComponent(allRTreeFiles.get(0).fullPath,
                    allBTreeFiles.get(0).fullPath));
            return validFiles;
        }

        // Sorts files names from earliest to latest timestamp.
        Collections.sort(allDeletedKeysBTreeFiles);
        Collections.sort(allBTreeFiles);

        List<ComparableFileName> validComparableRTreeFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastRTree = allRTreeFiles.get(0);
        validComparableRTreeFiles.add(lastRTree);

        List<ComparableFileName> validComparableBTreeFiles = new ArrayList<ComparableFileName>();
        ComparableFileName lastBTree = allBTreeFiles.get(0);
        validComparableBTreeFiles.add(lastBTree);

        for (int i = 1; i < allRTreeFiles.size(); i++) {
            ComparableFileName currentRTree = allRTreeFiles.get(i);
            ComparableFileName currentBTree = allBTreeFiles.get(i);
            // Current start timestamp is greater than last stop timestamp.
            if (currentRTree.interval[0].compareTo(lastRTree.interval[1]) > 0
                    && currentBTree.interval[0].compareTo(lastBTree.interval[1]) > 0) {
                validComparableRTreeFiles.add(currentRTree);
                validComparableBTreeFiles.add(currentBTree);
                lastRTree = currentRTree;
                lastBTree = currentBTree;
            } else if (currentRTree.interval[0].compareTo(lastRTree.interval[0]) >= 0
                    && currentRTree.interval[1].compareTo(lastRTree.interval[1]) <= 0
                    && currentBTree.interval[0].compareTo(lastBTree.interval[0]) >= 0
                    && currentBTree.interval[1].compareTo(lastBTree.interval[1]) <= 0) {
                // Invalid files are completely contained in last interval.
                File invalidRTreeFile = new File(currentRTree.fullPath);
                invalidRTreeFile.delete();
                File invalidBTreeFile = new File(currentBTree.fullPath);
                invalidBTreeFile.delete();
            } else {
                // This scenario should not be possible.
                throw new HyracksDataException("Found LSM files with overlapping but not contained timetamp intervals.");
            }
        }

        // Sort valid files in reverse lexicographical order, such that newer
        // files come first.
        Collections.sort(validComparableRTreeFiles, recencyCmp);
        Collections.sort(validComparableBTreeFiles, recencyCmp);

        Iterator<ComparableFileName> rtreeFileIter = validComparableRTreeFiles.iterator();
        Iterator<ComparableFileName> btreeFileIter = validComparableBTreeFiles.iterator();
        while (rtreeFileIter.hasNext() && btreeFileIter.hasNext()) {
            ComparableFileName cmpRTreeFileName = rtreeFileIter.next();
            ComparableFileName cmpBTreeFileName = btreeFileIter.next();
            validFiles.add(new LSMInvertedIndexFileNameComponent(cmpRTreeFileName.fullPath, cmpBTreeFileName.fullPath));
        }

        return validFiles;
    }

    public class LSMInvertedIndexFileNameComponent {
        private final String btreeFileName;
        private final String invListsFileName;
        private final String deletedKeysBTreeFileName;

        LSMInvertedIndexFileNameComponent(String btreeFileName, String invListsFileName, String deletedKeysBTreeFileName) {
            this.btreeFileName = btreeFileName;
            this.invListsFileName = invListsFileName;
            this.deletedKeysBTreeFileName = deletedKeysBTreeFileName;
        }

        public String getBTreeFileName() {
            return btreeFileName;
        }

        public String getInvListsFileName() {
            return invListsFileName;
        }

        public String getDeletedKeysBTreeFileName() {
            return deletedKeysBTreeFileName;
        }
    }
}
