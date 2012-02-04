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
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;

public class LSMTreeFileManager implements ILSMFileManager {

    private static final String SPLIT_STRING = "_";
    
    private final String baseDir;
    private final Format formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
    private final Comparator<String> cmp = new FileNameComparator();
    private final Comparator<String[]> intervalCmp = new IntervalComparator();
    
    public LSMTreeFileManager(String baseDir) {
        if (!baseDir.endsWith(System.getProperty("file.separator"))) {
            baseDir += System.getProperty("file.separator");
        }
        this.baseDir = baseDir;
    }
    
    @Override
    public String getFlushFileName() {
        Date date = new Date();
        String ts = formatter.format(date);
        // Begin timestamp and end timestamp are identical.
        return baseDir + ts + SPLIT_STRING + ts;
    }

    @Override
    public String getMergeFileName(String firstFileName, String lastFileName) throws HyracksDataException {        
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

    private class IntervalComparator implements Comparator<String[]> {
        @Override
        public int compare(String[] a, String[] b) {
            int startCmp = a[0].compareTo(b[0]);
            if (startCmp != 0) {
                return startCmp;
            }
            return b[1].compareTo(a[1]);
        }
    }
    
    @Override
    public String getBaseDir() {
        return baseDir;
    }

    @Override
    public List<String> cleanupAndGetValidFiles() throws HyracksDataException {
        List<String> validFiles = new ArrayList<String>();        
        File dir = new File(baseDir);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".");
            }
        };
        String[] files = dir.list(filter);
        // Trivial cases.
        if (files == null) {
            return validFiles;
        }
        if (files.length == 1) {
            validFiles.add(files[0]);
            return validFiles;
        }
        
        List<String[]> intervals = new ArrayList<String[]>(); 
        for (String fileName : files) {
            intervals.add(fileName.split(SPLIT_STRING));
        }
        // Sorts files from earliest to latest timestamp.
        Collections.sort(intervals, intervalCmp);
        
        String[] lastInterval = intervals.get(0);
        validFiles.add(getFileNameFromInterval(intervals.get(0)));
        for (int i = 1; i < intervals.size(); i++) {
            String[] currentInterval = intervals.get(i);
            // Current start timestamp is greater than last stop timestamp.
            if (currentInterval[0].compareTo(lastInterval[1]) > 0) {
                validFiles.add(getFileNameFromInterval(currentInterval));
                lastInterval = currentInterval;                
            } else if (currentInterval[0].compareTo(lastInterval[0]) >= 0 
                    && currentInterval[1].compareTo(lastInterval[1]) <= 0) {
                // Invalid files are completely contained in last interval.
                File invalidFile = new File(getFileNameFromInterval(currentInterval));
                invalidFile.delete();
            } else {
                // This scenario should not be possible.
                throw new HyracksDataException("Found LSM files with overlapping but not contained timetamp intervals.");
            }
        }
        // Sort valid files in reverse lexicographical order, such that newer files come first.
        Collections.sort(validFiles, cmp);
        return validFiles;
    }
    
    private String getFileNameFromInterval(String[] interval) {
        return baseDir + interval[0] + SPLIT_STRING + interval[1];
    }
}
