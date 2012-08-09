package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.io.File;
import java.io.FilenameFilter;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeFileManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndexFileManager extends LSMTreeFileManager {

    private static final String INVERTED_STRING = "i";
    private static final String BTREE_STRING = "b";

    private static FilenameFilter btreeFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(BTREE_STRING);
        }
    };
    
    private static FilenameFilter invertedFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return !name.startsWith(".") && name.endsWith(INVERTED_STRING);
        }
    };
    
    public LSMInvertedIndexFileManager(IOManager ioManager, IFileMapProvider fileMapProvider, String baseDir) {
        super(ioManager, fileMapProvider, baseDir);
    }

    @Override
    public Object getRelFlushFileName() {
        String baseName = (String) super.getRelFlushFileName();
        return new LSMInvertedFileNameComponent(baseName + SPLIT_STRING + INVERTED_STRING, baseName + SPLIT_STRING
                + BTREE_STRING);

    }

    @Override
    public Object getRelMergeFileName(String firstFileName, String lastFileName) throws HyracksDataException {
        String baseName = (String) super.getRelMergeFileName(firstFileName, lastFileName);
        return new LSMInvertedFileNameComponent(baseName + SPLIT_STRING + INVERTED_STRING, baseName + SPLIT_STRING
                + BTREE_STRING);
    }

//    @Override
//    public List<Object> cleanupAndGetValidFiles(Object lsmComponent, ILSMComponentFinalizer componentFinalizer) throws HyracksDataException {
//        List<Object> validFiles = new ArrayList<Object>();
//        ArrayList<ComparableFileName> allInvertedFiles = new ArrayList<ComparableFileName>();
//        ArrayList<ComparableFileName> allBTreeFiles = new ArrayList<ComparableFileName>();
//        LSMInvertedComponent component = (LSMInvertedComponent) lsmComponent;
//        
//        // Gather files from all IODeviceHandles.
//        for (IODeviceHandle dev : ioManager.getIODevices()) {            
//            getValidFiles(dev, btreeFilter, component.getBTree(), componentFinalizer, allBTreeFiles);
//            HashSet<String> btreeFilesSet = new HashSet<String>();
//            for (ComparableFileName cmpFileName : allBTreeFiles) {
//                int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
//                btreeFilesSet.add(cmpFileName.fileName.substring(0, index));
//            }
//            // List of valid Inverted files that may or may not have a BTree buddy. Will check for buddies below.
//            ArrayList<ComparableFileName> tmpAllInvertedFiles = new ArrayList<ComparableFileName>();
//            getValidFiles(dev, invertedFilter, component.getInverted(), componentFinalizer, tmpAllInvertedFiles);
//            // Look for buddy BTrees for all valid Inverteds. 
//            // If no buddy is found, delete the file, otherwise add the Inverted to allInvertedFiles. 
//            for (ComparableFileName cmpFileName : tmpAllInvertedFiles) {
//                int index = cmpFileName.fileName.lastIndexOf(SPLIT_STRING);
//                String file = cmpFileName.fileName.substring(0, index);
//                if (btreeFilesSet.contains(file)) {
//                    allInvertedFiles.add(cmpFileName);
//                } else {
//                    // Couldn't find the corresponding BTree file; thus, delete
//                    // the Inverted file.
//                    File invalidInvertedFile = new File(cmpFileName.fullPath);
//                    invalidInvertedFile.delete();
//                }
//            }
//        }
//        // Sanity check.
//        if (allInvertedFiles.size() != allBTreeFiles.size()) {
//            throw new HyracksDataException("Unequal number of valid Inverted and BTree files found. Aborting cleanup.");
//        }
//        
//        // Trivial cases.
//        if (allInvertedFiles.isEmpty() || allBTreeFiles.isEmpty()) {
//            return validFiles;
//        }
//
//        if (allInvertedFiles.size() == 1 && allBTreeFiles.size() == 1) {
//            validFiles.add(new LSMInvertedFileNameComponent(allInvertedFiles.get(0).fullPath, allBTreeFiles.get(0).fullPath));
//            return validFiles;
//        }
//
//        // Sorts files names from earliest to latest timestamp.
//        Collections.sort(allInvertedFiles);
//        Collections.sort(allBTreeFiles);
//
//        List<ComparableFileName> validComparableInvertedFiles = new ArrayList<ComparableFileName>();
//        ComparableFileName lastInverted = allInvertedFiles.get(0);
//        validComparableInvertedFiles.add(lastInverted);
//
//        List<ComparableFileName> validComparableBTreeFiles = new ArrayList<ComparableFileName>();
//        ComparableFileName lastBTree = allBTreeFiles.get(0);
//        validComparableBTreeFiles.add(lastBTree);
//
//        for (int i = 1; i < allInvertedFiles.size(); i++) {
//            ComparableFileName currentInverted = allInvertedFiles.get(i);
//            ComparableFileName currentBTree = allBTreeFiles.get(i);
//            // Current start timestamp is greater than last stop timestamp.
//            if (currentInverted.interval[0].compareTo(lastInverted.interval[1]) > 0
//                    && currentBTree.interval[0].compareTo(lastBTree.interval[1]) > 0) {
//                validComparableInvertedFiles.add(currentInverted);
//                validComparableBTreeFiles.add(currentBTree);
//                lastInverted = currentInverted;
//                lastBTree = currentBTree;
//            } else if (currentInverted.interval[0].compareTo(lastInverted.interval[0]) >= 0
//                    && currentInverted.interval[1].compareTo(lastInverted.interval[1]) <= 0
//                    && currentBTree.interval[0].compareTo(lastBTree.interval[0]) >= 0
//                    && currentBTree.interval[1].compareTo(lastBTree.interval[1]) <= 0) {
//                // Invalid files are completely contained in last interval.
//                File invalidInvertedFile = new File(currentInverted.fullPath);
//                invalidInvertedFile.delete();
//                File invalidBTreeFile = new File(currentBTree.fullPath);
//                invalidBTreeFile.delete();
//            } else {
//                // This scenario should not be possible.
//                throw new HyracksDataException("Found LSM files with overlapping but not contained timetamp intervals.");
//            }
//        }
//
//        // Sort valid files in reverse lexicographical order, such that newer
//        // files come first.
//        Collections.sort(validComparableInvertedFiles, recencyCmp);
//        Collections.sort(validComparableBTreeFiles, recencyCmp);
//
//        Iterator<ComparableFileName> invertedFileIter = validComparableInvertedFiles.iterator();
//        Iterator<ComparableFileName> btreeFileIter = validComparableBTreeFiles.iterator();
//        while (invertedFileIter.hasNext() && btreeFileIter.hasNext()) {
//            ComparableFileName cmpInvertedFileName = invertedFileIter.next();
//            ComparableFileName cmpBTreeFileName = btreeFileIter.next();
//            validFiles.add(new LSMInvertedFileNameComponent(cmpInvertedFileName.fullPath, cmpBTreeFileName.fullPath));
//        }
//
//        return validFiles;
//    }

    public class LSMInvertedFileNameComponent {
        private final String invertedFileName;
        private final String btreeFileName;

        LSMInvertedFileNameComponent(String invertedFileName, String btreeFileName) {
            this.invertedFileName = invertedFileName;
            this.btreeFileName = btreeFileName;
        }

        public String getInvertedFileName() {
            return invertedFileName;
        }

        public String getBTreeFileName() {
            return btreeFileName;
        }
    }

}
