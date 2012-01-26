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

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileNameManager;

public class LSMRTreeFileNameManager implements ILSMFileNameManager {

    private final String baseDir;
    private final Format formatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSS");
    private final Comparator<String> cmp = new FileNameComparator();
    
    public LSMRTreeFileNameManager(String baseDir) {
        if (!baseDir.endsWith(System.getProperty("file.separator"))) {
            baseDir += System.getProperty("file.separator");
        }
        this.baseDir = baseDir;
    }
    
    @Override
    public String getFlushFileName() {
        Date date = new Date();        
        // "Z" prefix to indicate flush. Relies on "Z" sorting higher than "A".
        return baseDir + "Z" + formatter.format(date);
    }

    @Override
    public String getMergeFileName() {
        Date date = new Date();
        // "A" prefix to indicate merge. Relies on "A" sorting lower than "Z".
        return baseDir + "A" + formatter.format(date);
    }

    @Override
    public Comparator<String> getFileNameComparator() {
        return cmp;
    }

    /**
     * Sorts strings in reverse lexicographical order. The way we construct the
     * file names above guarantees that:
     * 
     * 1. Flushed files ("Z" prefix) sort lower than merged files ("A" prefix)
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
}
