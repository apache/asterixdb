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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeFileManager;

public class LSMRTreeFileManager extends LSMTreeFileManager {
    private static final String RTREE_STRING = "r";
    private static final String BTREE_STRING = "b";

    public LSMRTreeFileManager(IOManager ioManager, String baseDir) {
        super(ioManager, baseDir);
    }

    @Override
    public Object getRelFlushFileName() {
        String baseName = (String) super.getRelFlushFileName();
        return new LSMRTreeFileNameComponent(baseName + SPLIT_STRING + RTREE_STRING, baseName + SPLIT_STRING
                + BTREE_STRING);

    }

    @Override
    public Object getRelMergeFileName(String firstFileName, String lastFileName) throws HyracksDataException {
        String baseName = (String) super.getRelMergeFileName(firstFileName, lastFileName);
        return new LSMRTreeFileNameComponent(baseName + SPLIT_STRING + RTREE_STRING, baseName + SPLIT_STRING
                + BTREE_STRING);
    }

    private boolean searchForFileName(HashSet<String> stringSet, String file) {
        if (stringSet.contains(file)) {
            return true;
        }
        return false;

    }

    public List<Object> cleanupAndGetValidFiles() throws HyracksDataException {
        List<Object> validFiles = new ArrayList<Object>();
        ArrayList<ComparableFileName> allRTreeFiles = new ArrayList<ComparableFileName>();
        ArrayList<ComparableFileName> allBTreeFiles = new ArrayList<ComparableFileName>();

        // Gather files from all IODeviceHandles.
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            File dir = new File(dev.getPath(), baseDir);
            FilenameFilter btreeFilter = new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return !name.startsWith(".") && name.endsWith(BTREE_STRING);
                }
            };
            String[] btreeFiles = dir.list(btreeFilter);
            for (String file : btreeFiles) {
                allBTreeFiles.add(new ComparableFileName(dir.getPath() + File.separator + file));
            }
            HashSet<String> btreeFilesSet = new HashSet<String>();
            for (String file : btreeFiles) {
                int index = file.lastIndexOf(SPLIT_STRING);
                btreeFilesSet.add(file.substring(0, index));
            }

            FilenameFilter rtreeFilter = new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return !name.startsWith(".") && name.endsWith(RTREE_STRING);
                }
            };
            String[] rtreeFiles = dir.list(rtreeFilter);
            for (String file : rtreeFiles) {
                int index = file.lastIndexOf(SPLIT_STRING);
                file = file.substring(0, index);
                if (searchForFileName(btreeFilesSet, file)) {
                    allRTreeFiles.add(new ComparableFileName(dir.getPath() + File.separator + file));
                } else {
                    // Couldn't find the corresponding BTree file; thus, delete
                    // the RTree file.
                    File invalidRTreeFile = new File(dir.getPath() + File.separator + file);
                    invalidRTreeFile.delete();
                }
            }
        }
        // Trivial cases.
        if (allRTreeFiles.isEmpty() || allBTreeFiles.isEmpty()) {
            return validFiles;
        }

        if (allRTreeFiles.size() == 1 && allBTreeFiles.size() == 1) {
            validFiles.add(new LSMRTreeFileNameComponent(allRTreeFiles.get(0).fullPath, allBTreeFiles.get(0).fullPath));
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
            validFiles.add(new LSMRTreeFileNameComponent(cmpRTreeFileName.fullPath, cmpBTreeFileName.fullPath));
        }

        return validFiles;
    }

    public class LSMRTreeFileNameComponent {
        private final String rtreeFileName;
        private final String btreeFileName;

        LSMRTreeFileNameComponent(String rtreeFileName, String btreeFileName) {
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
