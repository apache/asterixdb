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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;

public class LSMTreeFileManager implements ILSMFileManager {

    protected static final String SPLIT_STRING = "_";
    protected static final String TEMP_FILE_PREFIX = "lsm_tree";
    
    // Currently uses all IODevices registered in ioManager in a round-robin fashion.
    protected final IOManager ioManager;
    // baseDir should reflect dataset name, and partition name.
    protected final String baseDir;
    protected final Format formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");    
    protected final Comparator<String> cmp = new FileNameComparator();
    protected final Comparator<ComparableFileName> recencyCmp = new RecencyComparator();
    
    public LSMTreeFileManager(IOManager ioManager, String baseDir) {
        if (!baseDir.endsWith(System.getProperty("file.separator"))) {
            baseDir += System.getProperty("file.separator");
        }
        this.ioManager = ioManager;
        this.baseDir = baseDir;
        createDirs();
    }
    
    @Override
    public void createDirs() {
        for(IODeviceHandle dev : ioManager.getIODevices()) {
            File f = new File(dev.getPath(), baseDir);
            f.mkdirs();
        }
    }
    
    @Override
    public FileReference createTempFile() throws HyracksDataException {
        // Cycles through the IODevices in round-robin fashion.
        return ioManager.createWorkspaceFile(TEMP_FILE_PREFIX);
    }
    
    // Atomically renames src fileref to dest on same IODevice as src, and returns file ref of dest.
    @Override
    public FileReference rename(FileReference src, String dest) throws HyracksDataException {
        FileReference destFile = new FileReference(src.getDevideHandle(), dest);
        //try {
            //Files.move(src.getFile().toPath(), destFile.getFile().toPath(), StandardCopyOption.ATOMIC_MOVE);
            src.getFile().renameTo(destFile.getFile());
        //} catch (IOException e) {
        //    throw new HyracksDataException(e);
        //}
        return destFile;
    }
    
    @Override
    public Object getFlushFileName() {
        Date date = new Date();
        String ts = formatter.format(date);
        // Begin timestamp and end timestamp are identical.
        return baseDir + ts + SPLIT_STRING + ts;
    }

    @Override
    public Object getMergeFileName(String firstFileName, String lastFileName) throws HyracksDataException {        
        String[] firstTimestampRange = firstFileName.split(SPLIT_STRING);
        String[] lastTimestampRange = lastFileName.split(SPLIT_STRING);
        // Enclosing timestamp range.
        return baseDir + firstTimestampRange[0] + SPLIT_STRING + lastTimestampRange[1];
    }

    @Override
    public Comparator<String> getFileNameComparator() {
        return cmp;
    }

    /**
     * Sorts strings in reverse lexicographical order. The way we construct the
     * file names above guarantees that:
     * 
     * 1. Flushed files (sort lower than merged files
     * 
     * 2. Flushed files are sorted from newest to oldest (based on the timestamp
     * string)
     * 
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

    @Override
    public List<Object> cleanupAndGetValidFiles() throws HyracksDataException {
        List<Object> validFiles = new ArrayList<Object>();
        ArrayList<ComparableFileName> allFiles = new ArrayList<ComparableFileName>();
        // Gather files from all IODeviceHandles.
        for(IODeviceHandle dev : ioManager.getIODevices()) {
            File dir = new File(dev.getPath(), baseDir);
            FilenameFilter filter = new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return !name.startsWith(".");
                }
            };
            String[] files = dir.list(filter);
            for (String file : files) {
                allFiles.add(new ComparableFileName(dir.getPath() + File.separator + file));
            }
        }
        // Trivial cases.
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
            // Current start timestamp is greater than last stop timestamp.
            if (current.interval[0].compareTo(last.interval[1]) > 0) {
                validComparableFiles.add(current);
                last = current;                
            } else if (current.interval[0].compareTo(last.interval[0]) >= 0 
                    && current.interval[1].compareTo(last.interval[1]) <= 0) {
                // Invalid files are completely contained in last interval.
                File invalidFile = new File(current.fullPath);
                invalidFile.delete();
            } else {
                // This scenario should not be possible.
                throw new HyracksDataException("Found LSM files with overlapping but not contained timetamp intervals.");
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
        // Timestamp interval.
        public final String[] interval;
        
        public ComparableFileName(String fullPath) {
            this.fullPath = fullPath;
            File f = new File(fullPath);
            interval = f.getName().split(SPLIT_STRING);
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
    public IOManager getIOManager() {
        return ioManager;
    }
}
