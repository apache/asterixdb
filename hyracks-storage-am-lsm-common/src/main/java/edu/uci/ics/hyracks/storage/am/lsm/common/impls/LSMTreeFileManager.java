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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMTreeFileManager implements ILSMFileManager {

    protected static final String SPLIT_STRING = "_";

    // Use all IODevices registered in ioManager in a round-robin fashion to choose
    // where to flush and merge
    protected final IIOManager ioManager;
    protected final IFileMapProvider fileMapProvider;

    // baseDir should reflect dataset name and partition name.
    protected final String baseDir;
    protected final Format formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
    protected final Comparator<String> cmp = new FileNameComparator();
    protected final Comparator<ComparableFileName> recencyCmp = new RecencyComparator();

    // The current index for the round-robin file assignment
    private int ioDeviceIndex = 0;

    private static FilenameFilter fileNameFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".");
        }
    };

    public LSMTreeFileManager(IIOManager ioManager, IFileMapProvider fileMapProvider, String baseDir) {
        if (!baseDir.endsWith(System.getProperty("file.separator"))) {
            baseDir += System.getProperty("file.separator");
        }
        this.fileMapProvider = fileMapProvider;
        this.ioManager = ioManager;
        this.baseDir = baseDir;
        createDirs();
    }

    @Override
    public void createDirs() {
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            File f = new File(dev.getPath(), baseDir);
            f.mkdirs();
        }
    }

    public FileReference createFlushFile(String relFlushFileName) {
        // Assigns new files to I/O devices in round-robin fashion.
        IODeviceHandle dev = ioManager.getIODevices().get(ioDeviceIndex);
        ioDeviceIndex = (ioDeviceIndex + 1) % ioManager.getIODevices().size();
        return dev.createFileReference(relFlushFileName);
    }

    public FileReference createMergeFile(String relMergeFileName) {
        return createFlushFile(relMergeFileName);
    }

    @Override
    public Object getRelFlushFileName() {
        Date date = new Date();
        String ts = formatter.format(date);
        // Begin timestamp and end timestamp are identical since it is a flush
        return baseDir + ts + SPLIT_STRING + ts;
    }

    @Override
    public Object getRelMergeFileName(String firstFileName, String lastFileName) throws HyracksDataException {
        String[] firstTimestampRange = firstFileName.split(SPLIT_STRING);
        String[] lastTimestampRange = lastFileName.split(SPLIT_STRING);
        // Get the range of timestamps by taking the earliest and the latest timestamps
        return baseDir + firstTimestampRange[0] + SPLIT_STRING + lastTimestampRange[1];
    }

    @Override
    public Comparator<String> getFileNameComparator() {
        return cmp;
    }

    /**
     * Sorts strings in reverse lexicographical order. The way we construct the
     * file names above guarantees that:
     * 1. Flushed files sort lower than merged files
     * 2. Flushed files are sorted from newest to oldest (based on the timestamp
     * string)
     */
    private class FileNameComparator implements Comparator<String> {
        @Override
        public int compare(String a, String b) {
            // Consciously ignoring locale.
            return -a.compareTo(b);
        }
    }

    @Override
    public String getBaseDir() {
        return baseDir;
    }

    protected void cleanupAndGetValidFilesInternal(IODeviceHandle dev, FilenameFilter filter, Object lsmComponent,
            ILSMComponentFinalizer componentFinalizer, ArrayList<ComparableFileName> allFiles)
            throws HyracksDataException {
        File dir = new File(dev.getPath(), baseDir);
        String[] files = dir.list(filter);
        for (String fileName : files) {
            File file = new File(dir.getPath() + File.separator + fileName);
            if (componentFinalizer.isValid(file, lsmComponent)) {
                allFiles.add(new ComparableFileName(file.getAbsolutePath()));
            } else {
                file.delete();
            }
        }
    }

    @Override
    public List<Object> cleanupAndGetValidFiles(Object lsmComponent, ILSMComponentFinalizer componentFinalizer)
            throws HyracksDataException {
        List<Object> validFiles = new ArrayList<Object>();
        ArrayList<ComparableFileName> allFiles = new ArrayList<ComparableFileName>();

        // Gather files from all IODeviceHandles and delete invalid files
        // There are two types of invalid files:
        // (1) The isValid flag is not set
        // (2) The file's interval is contained by some other file
        // Here, we only filter out (1).
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            cleanupAndGetValidFilesInternal(dev, fileNameFilter, lsmComponent, componentFinalizer, allFiles);
        }

        if (allFiles.isEmpty()) {
            return validFiles;
        }

        if (allFiles.size() == 1) {
            validFiles.add(allFiles.get(0).fullPath);
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
                File invalidFile = new File(current.fullPath);
                invalidFile.delete();
            } else {
                // This scenario should not be possible since timestamps are monotonically increasing.
                throw new HyracksDataException("Found LSM files with overlapping timestamp intervals, "
                        + "but the intervals were not contained by another file.");
            }
        }

        // Sort valid files in reverse lexicographical order, such that newer files come first.
        Collections.sort(validComparableFiles, recencyCmp);
        for (ComparableFileName cmpFileName : validComparableFiles) {
            validFiles.add(cmpFileName.fullPath);
        }

        return validFiles;
    }

    protected class ComparableFileName implements Comparable<ComparableFileName> {
        public final String fullPath;
        public final String fileName;

        // Timestamp interval.
        public final String[] interval;

        public ComparableFileName(String fullPath) {
            this.fullPath = fullPath;
            File f = new File(fullPath);
            this.fileName = f.getName();
            interval = fileName.split(SPLIT_STRING);
        }

        @Override
        public int compareTo(ComparableFileName b) {
            int startCmp = interval[0].compareTo(b.interval[0]);
            if (startCmp != 0) {
                return startCmp;
            }
            return b.interval[1].compareTo(interval[1]);
        }
    }

    private class RecencyComparator implements Comparator<ComparableFileName> {
        @Override
        public int compare(ComparableFileName a, ComparableFileName b) {
            int cmp = -a.interval[0].compareTo(b.interval[0]);
            if (cmp != 0) {
                return cmp;
            }
            return -a.interval[1].compareTo(b.interval[1]);
        }
    }

    @Override
    public IIOManager getIOManager() {
        return ioManager;
    }
}
