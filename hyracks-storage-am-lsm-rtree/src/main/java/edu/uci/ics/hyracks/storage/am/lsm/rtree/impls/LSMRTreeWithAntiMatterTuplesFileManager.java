/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import java.util.Date;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTreeWithAntiMatterTuplesFileManager extends AbstractLSMIndexFileManager {

    private final TreeIndexFactory<? extends ITreeIndex> rtreeFactory;

    public LSMRTreeWithAntiMatterTuplesFileManager(IIOManager ioManager, IFileMapProvider fileMapProvider,
            FileReference file, TreeIndexFactory<? extends ITreeIndex> rtreeFactory, int startIODeviceIndex) {
        super(ioManager, fileMapProvider, file, null, startIODeviceIndex);
        this.rtreeFactory = rtreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() {
        Date date = new Date();
        String ts = formatter.format(date);
        // Begin timestamp and end timestamp are identical since it is a flush
        return new LSMComponentFileReferences(createFlushFile(baseDir + ts + SPLIT_STRING + ts), null, null);
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName)
            throws HyracksDataException {
        String[] firstTimestampRange = firstFileName.split(SPLIT_STRING);
        String[] lastTimestampRange = lastFileName.split(SPLIT_STRING);
        // Get the range of timestamps by taking the earliest and the latest timestamps
        return new LSMComponentFileReferences(createMergeFile(baseDir + firstTimestampRange[0] + SPLIT_STRING
                + lastTimestampRange[1]), null, null);
    }

    private static FilenameFilter fileNameFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".");
        }
    };

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException, IndexException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<LSMComponentFileReferences>();
        ArrayList<ComparableFileName> allFiles = new ArrayList<ComparableFileName>();

        // Gather files from all IODeviceHandles and delete invalid files
        // There are two types of invalid files:
        // (1) The isValid flag is not set
        // (2) The file's interval is contained by some other file
        // Here, we only filter out (1).
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            cleanupAndGetValidFilesInternal(dev, fileNameFilter, rtreeFactory, allFiles);
        }

        if (allFiles.isEmpty()) {
            return validFiles;
        }

        if (allFiles.size() == 1) {
            validFiles.add(new LSMComponentFileReferences(allFiles.get(0).fileRef, null, null));
            return validFiles;
        }

        // Sorts files names from earliest to latest timestamp.
        Collections.sort(allFiles);

        List<ComparableFileName> validComparableFiles = new ArrayList<ComparableFileName>();
        ComparableFileName last = allFiles.get(0);
        validComparableFiles.add(last);
        for (int i = 1; i < allFiles.size(); i++) {
            ComparableFileName current = allFiles.get(i);
            // The current start timestamp is greater than last stop timestamp so current is valid.
            if (current.interval[0].compareTo(last.interval[1]) > 0) {
                validComparableFiles.add(current);
                last = current;
            } else if (current.interval[0].compareTo(last.interval[0]) >= 0
                    && current.interval[1].compareTo(last.interval[1]) <= 0) {
                // The current file is completely contained in the interval of the 
                // last file. Thus the last file must contain at least as much information 
                // as the current file, so delete the current file.
                current.fileRef.delete();
            } else {
                // This scenario should not be possible since timestamps are monotonically increasing.
                throw new HyracksDataException("Found LSM files with overlapping timestamp intervals, "
                        + "but the intervals were not contained by another file.");
            }
        }

        // Sort valid files in reverse lexicographical order, such that newer files come first.
        Collections.sort(validComparableFiles, recencyCmp);
        for (ComparableFileName cmpFileName : validComparableFiles) {
            validFiles.add(new LSMComponentFileReferences(cmpFileName.fileRef, null, null));
        }

        return validFiles;
    }
}
