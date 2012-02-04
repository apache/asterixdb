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

package edu.uci.ics.hyracks.storage.am.lsm.common;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeFileManager;

public class LSMTreeFileManagerTest {
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected String baseDir;

    @Before
    public void setUp() throws HyracksDataException {
        baseDir = tmpDir + sep + "lsm_tree" + simpleDateFormat.format(new Date()) + sep;
        File f = new File(baseDir);
        f.mkdirs();
    }
    
    @After
    public void tearDown() throws HyracksDataException {
        baseDir = tmpDir + sep + "lsm_tree" + simpleDateFormat.format(new Date()) + sep;
        File f = new File(baseDir);
        f.deleteOnExit();
    }

    public void sortOrderTest(boolean testFlushFileName) throws InterruptedException, HyracksDataException {
        ILSMFileManager fileNameManager = new LSMTreeFileManager(baseDir);
        LinkedList<String> fileNames = new LinkedList<String>();

        int numFileNames = 100;
        long sleepTime = 5;
        for (int i = 0; i < numFileNames; i++) {
            String flushFileName = fileNameManager.getFlushFileName();
            if (testFlushFileName) {
                fileNames.addFirst(flushFileName);
            }
            Thread.sleep(sleepTime);
            if (!testFlushFileName) {
                String secondFlushFileName = fileNameManager.getFlushFileName();
                String mergeFileName = getMergeFileName(fileNameManager, flushFileName, secondFlushFileName);
                fileNames.addFirst(mergeFileName);
                Thread.sleep(sleepTime);
            }
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

    //@Test
    public void flushAndMergeFilesSortOrderTest() throws InterruptedException, HyracksDataException {
        sortOrderTest(true);
        sortOrderTest(false);
    }
    
    @Test
    public void cleanInvalidFilesTest() throws InterruptedException, IOException {
        ILSMFileManager fileNameManager = new LSMTreeFileManager(baseDir);
        List<String> flushFileNames = new ArrayList<String>();
        List<String> allFiles = new ArrayList<String>();
        
        int numFileNames = 100;
        long sleepTime = 5;
        // Generate a bunch of flush files.
        for (int i = 0; i < numFileNames; i++) {
            String flushFileName = fileNameManager.getFlushFileName();            
            flushFileNames.add(flushFileName);
            Thread.sleep(sleepTime);
        }
        allFiles.addAll(flushFileNames);
        
        // Simulate merging some of the flush files.
        // Merge range 0 to 4.
        String mergeFile1 = getMergeFileName(fileNameManager, flushFileNames.get(0), flushFileNames.get(4));
        allFiles.add(mergeFile1);
        // Merge range 5 to 9.
        String mergeFile2 = getMergeFileName(fileNameManager, flushFileNames.get(5), flushFileNames.get(9));
        allFiles.add(mergeFile2);
        // Merge range 10 to 19.
        String mergeFile3 = getMergeFileName(fileNameManager, flushFileNames.get(10), flushFileNames.get(19));
        allFiles.add(mergeFile3);
        // Merge range 20 to 29.
        String mergeFile4 = getMergeFileName(fileNameManager, flushFileNames.get(20), flushFileNames.get(29));
        allFiles.add(mergeFile4);
        // Merge range 50 to 79.
        String mergeFile5 = getMergeFileName(fileNameManager, flushFileNames.get(50), flushFileNames.get(79));
        allFiles.add(mergeFile5);
        
        // Simulate merging of merge files.
        String mergeFile6 = getMergeFileName(fileNameManager, mergeFile1, mergeFile2);
        allFiles.add(mergeFile6);
        String mergeFile7 = getMergeFileName(fileNameManager, mergeFile3, mergeFile4);
        allFiles.add(mergeFile7);
        
        // Create all files.
        for (String fileName : allFiles) {
            File f = new File(fileName);
            f.createNewFile();
            f.deleteOnExit();
        }
        
        // Populate expected valid flush files.
        List<String> expectedValidFiles = new ArrayList<String>();
        for (int i = 30; i < 50; i++) {
            expectedValidFiles.add(flushFileNames.get(i));
        }
        for (int i = 80; i < 100; i++) {
            expectedValidFiles.add(flushFileNames.get(i));
        }
        
        // Populate expected valid merge files.
        expectedValidFiles.add(mergeFile5);
        expectedValidFiles.add(mergeFile6);
        expectedValidFiles.add(mergeFile7);

        // Sort expected files.
        Collections.sort(expectedValidFiles, fileNameManager.getFileNameComparator());
        
        List<String> validFiles = fileNameManager.cleanupAndGetValidFiles();
        
        // Check actual files against expected files.
        assertEquals(expectedValidFiles.size(), validFiles.size());
        for (int i = 0; i < expectedValidFiles.size(); i++) {
            assertEquals(expectedValidFiles.get(i), validFiles.get(i));
        }
        
        // Make sure invalid files were removed from baseDir.
        File dir = new File(baseDir);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".");
            }
        };
        String[] files = dir.list(filter);
        Arrays.sort(files, fileNameManager.getFileNameComparator());
        // Check actual files in directory against expected files.
        assertEquals(expectedValidFiles.size(), files.length);
        for (int i = 0; i < expectedValidFiles.size(); i++) {
            assertEquals(expectedValidFiles.get(i), baseDir + files[i]);            
        }
        
        // Cleanup.
        for (String fileName : files) {
            File f = new File(fileName);
            f.deleteOnExit();
        }
    }
    
    private String getMergeFileName(ILSMFileManager fileNameManager, String firstFile, String lastFile) throws HyracksDataException {
        File f1 = new File(firstFile);
        File f2 = new File(lastFile);
        return fileNameManager.getMergeFileName(f1.getName(), f2.getName());
    }
}
