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
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

// TODO: Implement this one properly!
public class LSMInvertedIndexFileManager extends LSMTreeFileManager {
    private static final String RTREE_STRING = "r";
    private static final String BTREE_STRING = "b";

    private final TreeIndexFactory<? extends ITreeIndex> rtreeFactory;
    private final TreeIndexFactory<? extends ITreeIndex> btreeFactory;

    private static FilenameFilter btreeFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(BTREE_STRING);
        }
    };

    private static FilenameFilter rtreeFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(RTREE_STRING);
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
        return new LSMRInvertedIndexFileNameComponent(baseName + SPLIT_STRING + RTREE_STRING, baseName + SPLIT_STRING
                + BTREE_STRING);

    }

    @Override
    public Object getRelMergeFileName(String firstFileName, String lastFileName) throws HyracksDataException {
        String baseName = (String) super.getRelMergeFileName(firstFileName, lastFileName);
        return new LSMRInvertedIndexFileNameComponent(baseName + SPLIT_STRING + RTREE_STRING, baseName + SPLIT_STRING
                + BTREE_STRING);
    }

    @Override
    public List<Object> cleanupAndGetValidFiles(ILSMComponentFinalizer componentFinalizer) throws HyracksDataException {
        List<Object> validFiles = new ArrayList<Object>();
        ArrayList<ComparableFileName> allRTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allBTreeFiles = new ArrayList<ComparableFileName>();

        // Gather files from all IODeviceHandles.
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            cleanupAndGetValidFilesInternal(dev, btreeFilter, btreeFactory, componentFinalizer, allBTreeFiles);
            HashSet<String> btreeFilesSet = new HashSet<String>();
            for (ComparableFileName cmpFileName : allBTreeFiles) {
                int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
                btreeFilesSet.add(cmpFileName.fileName.substring(0, index));
            }
            // List of valid RTree files that may or may not have a BTree buddy. Will check for buddies below.
            ArrayList<ComparableFileName> tmpAllRTreeFiles = new ArrayList<ComparableFileName>();
            cleanupAndGetValidFilesInternal(dev, rtreeFilter, rtreeFactory, componentFinalizer, tmpAllRTreeFiles);
            // Look for buddy BTrees for all valid RTrees. 
            // If no buddy is found, delete the file, otherwise add the RTree to allRTreeFiles. 
            for (ComparableFileName cmpFileName : tmpAllRTreeFiles) {
                int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
                String file = cmpFileName.fileName.substring(0, index);
                if (btreeFilesSet.contains(file)) {
                    allRTreeFiles.add(cmpFileName);
                } else {
                    // Couldn't find the corresponding BTree file; thus, delete
                    // the RTree file.
                    File invalidRTreeFile = new File(cmpFileName.fullPath);
                    invalidRTreeFile.delete();
                }
            }
        }
        // Sanity check.
        if (allRTreeFiles.size() != allBTreeFiles.size()) {
            throw new HyracksDataException("Unequal number of valid RTree and BTree files found. Aborting cleanup.");
        }

        // Trivial cases.
        if (allRTreeFiles.isEmpty() || allBTreeFiles.isEmpty()) {
            return validFiles;
        }

        if (allRTreeFiles.size() == 1 && allBTreeFiles.size() == 1) {
            validFiles.add(new LSMRInvertedIndexFileNameComponent(allRTreeFiles.get(0).fullPath, allBTreeFiles.get(0).fullPath));
            return validFiles;
        }

        // Sorts files names from earliest to latest timestamp.
        Collections.sort(allRTreeFiles);
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
            validFiles.add(new LSMRInvertedIndexFileNameComponent(cmpRTreeFileName.fullPath, cmpBTreeFileName.fullPath));
        }

        return validFiles;
    }

    public class LSMRInvertedIndexFileNameComponent {
        private final String rtreeFileName;
        private final String btreeFileName;

        LSMRInvertedIndexFileNameComponent(String rtreeFileName, String btreeFileName) {
            this.rtreeFileName = rtreeFileName;
            this.btreeFileName = btreeFileName;
        }

        public String getRTreeFileName() {
            return rtreeFileName;
        }

        public String getBTreeFileName() {
            return btreeFileName;
        }
    }
}
