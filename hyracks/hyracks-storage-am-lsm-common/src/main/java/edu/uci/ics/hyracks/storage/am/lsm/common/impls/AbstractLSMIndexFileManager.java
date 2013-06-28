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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractLSMIndexFileManager implements ILSMIndexFileManager {

    protected static final String SPLIT_STRING = "_";
    protected static final String BLOOM_FILTER_STRING = "f";

    protected final IFileMapProvider fileMapProvider;

    // baseDir should reflect dataset name and partition name.
    protected String baseDir;
    protected final Format formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
    protected final Comparator<String> cmp = new FileNameComparator();
    protected final Comparator<ComparableFileName> recencyCmp = new RecencyComparator();

    protected final TreeIndexFactory<? extends ITreeIndex> treeFactory;

    public AbstractLSMIndexFileManager(IFileMapProvider fileMapProvider, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> treeFactory) {
        this.baseDir = file.getFile().getPath();
        if (!baseDir.endsWith(System.getProperty("file.separator"))) {
            baseDir += System.getProperty("file.separator");
        }
        this.fileMapProvider = fileMapProvider;
        this.treeFactory = treeFactory;
    }

    private static FilenameFilter fileNameFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".");
        }
    };

    protected boolean isValidTreeIndex(ITreeIndex treeIndex) throws HyracksDataException {
        IBufferCache bufferCache = treeIndex.getBufferCache();
        treeIndex.activate();
        try {
            int metadataPage = treeIndex.getFreePageManager().getFirstMetadataPage();
            ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory()
                    .createFrame();
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(treeIndex.getFileId(), metadataPage),
                    false);
            page.acquireReadLatch();
            try {
                metadataFrame.setPage(page);
                return metadataFrame.isValid();
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        } finally {
            treeIndex.deactivate();
        }
    }

    protected void cleanupAndGetValidFilesInternal(FilenameFilter filter,
            TreeIndexFactory<? extends ITreeIndex> treeFactory, ArrayList<ComparableFileName> allFiles)
            throws HyracksDataException, IndexException {
        File dir = new File(baseDir);
        String[] files = dir.list(filter);
        for (String fileName : files) {
            File file = new File(dir.getPath() + File.separator + fileName);
            FileReference fileRef = new FileReference(file);
            if (treeFactory == null || isValidTreeIndex(treeFactory.createIndexInstance(fileRef))) {
                allFiles.add(new ComparableFileName(fileRef));
            } else {
                file.delete();
            }
        }
    }

    protected void validateFiles(HashSet<String> groundTruth, ArrayList<ComparableFileName> validFiles,
            FilenameFilter filter, TreeIndexFactory<? extends ITreeIndex> treeFactory) throws HyracksDataException,
            IndexException {
        ArrayList<ComparableFileName> tmpAllInvListsFiles = new ArrayList<ComparableFileName>();
        cleanupAndGetValidFilesInternal(filter, treeFactory, tmpAllInvListsFiles);
        for (ComparableFileName cmpFileName : tmpAllInvListsFiles) {
            int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
            String file = cmpFileName.fileName.substring(0, index);
            if (groundTruth.contains(file)) {
                validFiles.add(cmpFileName);
            } else {
                File invalidFile = new File(cmpFileName.fullPath);
                invalidFile.delete();
            }
        }
    }

    @Override
    public void createDirs() {
        File f = new File(baseDir);
        f.mkdirs();
    }

    @Override
    public void deleteDirs() {
        File f = new File(baseDir);
        delete(f);
    }

    private void delete(File f) {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                delete(c);
            }
        }
        f.delete();
    }

    protected static FilenameFilter bloomFilterFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(BLOOM_FILTER_STRING);
        }
    };

    protected FileReference createFlushFile(String relFlushFileName) {
        return new FileReference(new File(relFlushFileName));
    }

    protected FileReference createMergeFile(String relMergeFileName) {
        return createFlushFile(relMergeFileName);
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

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException, IndexException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<LSMComponentFileReferences>();
        ArrayList<ComparableFileName> allFiles = new ArrayList<ComparableFileName>();

        // Gather files and delete invalid files
        // There are two types of invalid files:
        // (1) The isValid flag is not set
        // (2) The file's interval is contained by some other file
        // Here, we only filter out (1).
        cleanupAndGetValidFilesInternal(fileNameFilter, treeFactory, allFiles);

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

    protected class ComparableFileName implements Comparable<ComparableFileName> {
        public final FileReference fileRef;
        public final String fullPath;
        public final String fileName;

        // Timestamp interval.
        public final String[] interval;

        public ComparableFileName(FileReference fileRef) {
            this.fileRef = fileRef;
            this.fullPath = fileRef.getFile().getAbsolutePath();
            this.fileName = fileRef.getFile().getName();
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
}
