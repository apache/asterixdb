/*
 * Copyright 2009-2013 by The Regents of the University of California
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;

public class LSMIndexFileManagerTest {
    private static final int DEFAULT_PAGE_SIZE = 256;
    private static final int DEFAULT_NUM_PAGES = 100;
    private static final int DEFAULT_MAX_OPEN_FILES = 10;
    private static final int DEFAULT_IO_DEVICE_ID = 0;
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String sep = System.getProperty("file.separator");
    protected IOManager ioManager;
    protected IFileMapProvider fileMapProvider;
    protected String baseDir;
    protected FileReference file;

    @Before
    public void setUp() throws HyracksException {
        TestStorageManagerComponentHolder.init(DEFAULT_PAGE_SIZE, DEFAULT_NUM_PAGES, DEFAULT_MAX_OPEN_FILES);
        ioManager = TestStorageManagerComponentHolder.getIOManager();
        fileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(null);
        baseDir = ioManager.getIODevices().get(DEFAULT_IO_DEVICE_ID).getPath() + sep + "lsm_tree"
                + simpleDateFormat.format(new Date()) + sep;
        File f = new File(baseDir);
        f.mkdirs();
        file = new FileReference(f);
    }

    @After
    public void tearDown() throws HyracksDataException {
        File f = new File(baseDir);
        f.deleteOnExit();
    }

    public void sortOrderTest(boolean testFlushFileName) throws InterruptedException, HyracksDataException {
        ILSMIndexFileManager fileManager = new DummyLSMIndexFileManager(fileMapProvider, file, new DummyTreeFactory());
        LinkedList<String> fileNames = new LinkedList<String>();

        int numFileNames = 100;
        long sleepTime = 5;
        for (int i = 0; i < numFileNames; i++) {
            String flushFileName = (String) fileManager.getRelFlushFileReference().getInsertIndexFileReference()
                    .getFile().getName();
            if (testFlushFileName) {
                fileNames.addFirst(flushFileName);
            }
            Thread.sleep(sleepTime);
            if (!testFlushFileName) {
                String secondFlushFileName = (String) fileManager.getRelFlushFileReference()
                        .getInsertIndexFileReference().getFile().getName();
                String mergeFileName = getMergeFileName(fileManager, flushFileName, secondFlushFileName);
                fileNames.addFirst(mergeFileName);
                Thread.sleep(sleepTime);
            }
        }

        List<String> sortedFileNames = new ArrayList<String>();
        sortedFileNames.addAll(fileNames);

        // Make sure the comparator sorts in the correct order (i.e., the
        // reverse insertion order in this case).
        Comparator<String> cmp = fileManager.getFileNameComparator();
        Collections.sort(sortedFileNames, cmp);
        for (int i = 0; i < numFileNames; i++) {
            assertEquals(fileNames.get(i), sortedFileNames.get(i));
        }
    }

    @Test
    public void flushAndMergeFilesSortOrderTest() throws InterruptedException, HyracksDataException {
        sortOrderTest(true);
        sortOrderTest(false);
    }

    public void cleanInvalidFilesTest(IOManager ioManager) throws InterruptedException, IOException, IndexException {
        ILSMIndexFileManager fileManager = new DummyLSMIndexFileManager(fileMapProvider, file, new DummyTreeFactory());
        fileManager.createDirs();

        List<FileReference> flushFiles = new ArrayList<FileReference>();
        List<FileReference> allFiles = new ArrayList<FileReference>();

        int numFileNames = 100;
        long sleepTime = 5;
        // Generate a bunch of flush files.
        for (int i = 0; i < numFileNames; i++) {
            LSMComponentFileReferences relFlushFileRefs = fileManager.getRelFlushFileReference();
            flushFiles.add(relFlushFileRefs.getInsertIndexFileReference());
            Thread.sleep(sleepTime);
        }
        allFiles.addAll(flushFiles);

        // Simulate merging some of the flush files.
        // Merge range 0 to 4.
        FileReference mergeFile1 = simulateMerge(fileManager, flushFiles.get(0), flushFiles.get(4));
        allFiles.add(mergeFile1);
        // Merge range 5 to 9.
        FileReference mergeFile2 = simulateMerge(fileManager, flushFiles.get(5), flushFiles.get(9));
        allFiles.add(mergeFile2);
        // Merge range 10 to 19.
        FileReference mergeFile3 = simulateMerge(fileManager, flushFiles.get(10), flushFiles.get(19));
        allFiles.add(mergeFile3);
        // Merge range 20 to 29.
        FileReference mergeFile4 = simulateMerge(fileManager, flushFiles.get(20), flushFiles.get(29));
        allFiles.add(mergeFile4);
        // Merge range 50 to 79.
        FileReference mergeFile5 = simulateMerge(fileManager, flushFiles.get(50), flushFiles.get(79));
        allFiles.add(mergeFile5);

        // Simulate merging of merge files.
        FileReference mergeFile6 = simulateMerge(fileManager, mergeFile1, mergeFile2);
        allFiles.add(mergeFile6);
        FileReference mergeFile7 = simulateMerge(fileManager, mergeFile3, mergeFile4);
        allFiles.add(mergeFile7);

        // Create all files and set delete on exit for all files.
        for (FileReference fileRef : allFiles) {
            fileRef.getFile().createNewFile();
            fileRef.getFile().deleteOnExit();
        }

        // Populate expected valid flush files.
        List<String> expectedValidFiles = new ArrayList<String>();
        for (int i = 30; i < 50; i++) {
            expectedValidFiles.add(flushFiles.get(i).getFile().getName());
        }
        for (int i = 80; i < 100; i++) {
            expectedValidFiles.add(flushFiles.get(i).getFile().getName());
        }

        // Populate expected valid merge files.
        expectedValidFiles.add(mergeFile5.getFile().getName());
        expectedValidFiles.add(mergeFile6.getFile().getName());
        expectedValidFiles.add(mergeFile7.getFile().getName());

        // Sort expected files.
        Collections.sort(expectedValidFiles, fileManager.getFileNameComparator());

        // Pass null and a dummy component finalizer. We don't test for physical consistency in this test.
        List<LSMComponentFileReferences> lsmComonentFileReference = fileManager.cleanupAndGetValidFiles();

        // Check actual files against expected files.
        assertEquals(expectedValidFiles.size(), lsmComonentFileReference.size());
        for (int i = 0; i < expectedValidFiles.size(); i++) {
            assertEquals(expectedValidFiles.get(i), lsmComonentFileReference.get(i).getInsertIndexFileReference()
                    .getFile().getName());
        }

        // Make sure invalid files were removed from the IODevices.
        ArrayList<String> remainingFiles = new ArrayList<String>();
        File dir = new File(baseDir);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".");
            }
        };
        String[] files = dir.list(filter);
        for (String file : files) {
            File f = new File(file);
            remainingFiles.add(f.getName());
        }

        Collections.sort(remainingFiles, fileManager.getFileNameComparator());
        // Check actual files in directory against expected files.
        assertEquals(expectedValidFiles.size(), remainingFiles.size());
        for (int i = 0; i < expectedValidFiles.size(); i++) {
            assertEquals(expectedValidFiles.get(i), remainingFiles.get(i));
        }
    }

    @Test
    public void singleIODeviceTest() throws InterruptedException, IOException, IndexException {
        IOManager singleDeviceIOManager = createIOManager(1);
        cleanInvalidFilesTest(singleDeviceIOManager);
        cleanDirs(singleDeviceIOManager);
    }

    private void cleanDirs(IOManager ioManager) {
        File dir = new File(baseDir);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".");
            }
        };
        String[] files = dir.list(filter);
        for (String file : files) {
            File f = new File(file);
            f.delete();
        }

    }

    private IOManager createIOManager(int numDevices) throws HyracksException {
        List<IODeviceHandle> devices = new ArrayList<IODeviceHandle>();
        for (int i = 0; i < numDevices; i++) {
            String iodevPath = System.getProperty("java.io.tmpdir") + sep + "test_iodev" + i;
            devices.add(new IODeviceHandle(new File(iodevPath), "wa"));
        }
        return new IOManager(devices, Executors.newCachedThreadPool());
    }

    private FileReference simulateMerge(ILSMIndexFileManager fileManager, FileReference a, FileReference b)
            throws HyracksDataException {
        LSMComponentFileReferences relMergeFileRefs = fileManager.getRelMergeFileReference(a.getFile().getName(), b
                .getFile().getName());
        return relMergeFileRefs.getInsertIndexFileReference();
    }

    private String getMergeFileName(ILSMIndexFileManager fileNameManager, String firstFile, String lastFile)
            throws HyracksDataException {
        File f1 = new File(firstFile);
        File f2 = new File(lastFile);
        return (String) fileNameManager.getRelMergeFileReference(f1.getName(), f2.getName())
                .getInsertIndexFileReference().getFile().getName();
    }
}
