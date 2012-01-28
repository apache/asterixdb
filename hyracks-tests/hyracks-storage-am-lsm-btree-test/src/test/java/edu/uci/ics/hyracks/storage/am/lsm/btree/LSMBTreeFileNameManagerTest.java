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

package edu.uci.ics.hyracks.storage.am.lsm.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTreeFileNameManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileNameManager;

public class LSMBTreeFileNameManagerTest {
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected String baseDir;

    @Before
    public void setUp() throws HyracksDataException {
        baseDir = tmpDir + sep + "lsm_btree" + simpleDateFormat.format(new Date()) + sep;
    }

    public void flushFileNamesTest(boolean testFlushFileName) throws InterruptedException {
        ILSMFileNameManager fileNameManager = new LSMBTreeFileNameManager(baseDir);
        LinkedList<String> fileNames = new LinkedList<String>();

        int numFileNames = 100;
        long sleepTime = 5;
        for (int i = 0; i < numFileNames; i++) {
            String fileName = null;
            if (testFlushFileName) {
                fileName = fileNameManager.getFlushFileName();
            } else {
                fileName = fileNameManager.getMergeFileName();
            }
            fileNames.addFirst(fileName);
            Thread.sleep(sleepTime);
        }

        List<String> sortedFileNames = new ArrayList<String>();
        sortedFileNames.addAll(fileNames);        

        // Make sure the comparator sorts in the correct order (i.e., the
        // reverse insertion order in this case).
        Comparator<String> cmp = fileNameManager.getFileNameComparator();
        Collections.sort(sortedFileNames, cmp);
        for (int i = 0; i < numFileNames; i++) {
            assertEquals(fileNames.get(i), sortedFileNames.get(i));
        }
    }

    @Test
    public void individualFileNamesTest() throws InterruptedException {
        flushFileNamesTest(true);
        flushFileNamesTest(false);
    }

    @Test
    public void mixedFileNamesTest() throws InterruptedException {
        ILSMFileNameManager fileNameManager = new LSMBTreeFileNameManager(baseDir);
        List<String> fileNames = new ArrayList<String>();
        HashSet<String> flushFileNames = new HashSet<String>();
        HashSet<String> mergeFileNames = new HashSet<String>();
        
        int numFileNames = 100;
        long sleepTime = 5;
        for (int i = 0; i < numFileNames; i++) {
            String fileName = null;
            if (i % 2 == 0) {
                fileName = fileNameManager.getFlushFileName();
                flushFileNames.add(fileName);
            } else {
                fileName = fileNameManager.getMergeFileName();
                mergeFileNames.add(fileName);
            }
            fileNames.add(fileName);
            Thread.sleep(sleepTime);
        }

        // Make sure the comparator sorts in the correct order.
        // We only need to check whether the flushed files and merged files are
        // clustered.
        // We verified the secondary sort order (based on timestamp) in the
        // individualFileNamesTest().
        Comparator<String> cmp = fileNameManager.getFileNameComparator();
        Collections.sort(fileNames, cmp);
        // We expect flush file names at the front.
        int i = 0;
        while (flushFileNames.contains(fileNames.get(i))) {
            i++;
        }
        // We expect only merge file names from here on.
        while (i < numFileNames) {
            if (!mergeFileNames.contains(fileNames.get(i))) {
                fail("Expected a merge file name at position: " + i);
            }
            i++;
        }
    }
}
