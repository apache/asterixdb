package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import java.io.File;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class LSMInvertedIndexTestHarness {

    private static final long RANDOM_SEED = 50;
    private static final int DEFAULT_DISK_PAGE_SIZE = 256;
    private static final int DEFAULT_DISK_NUM_PAGES = 1000;
    private static final int DEFAULT_DISK_MAX_OPEN_FILES = 200;
    private static final int DEFAULT_MEM_PAGE_SIZE = 4096;
    private static final int DEFAULT_MEM_NUM_PAGES = 200;
    private static final int DEFAULT_HYRACKS_FRAME_SIZE = 128;
    private static final int DUMMY_FILE_ID = -1;

    protected final int diskPageSize;
    protected final int diskNumPages;
    protected final int diskMaxOpenFiles;
    protected final int memPageSize;
    protected final int memNumPages;
    protected final int hyracksFrameSize;

    protected IOManager ioManager;
    protected IBufferCache diskBufferCache;
    protected IFileMapProvider diskFileMapProvider;
    protected InMemoryBufferCache memBufferCache;
    protected InMemoryFreePageManager memFreePageManager;
    protected IHyracksTaskContext ctx;

    protected final Random rnd = new Random();
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String sep = System.getProperty("file.separator");
    protected String onDiskDir;
    protected String btreeFileName = "btree_vocab";
    protected String invIndexFileName = "inv_index";
    protected FileReference btreeFileRef;
    protected FileReference invIndexFileRef;

    // Token information
    protected ITypeTraits[] tokenTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS };
    protected IBinaryComparatorFactory[] tokenCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY) };

    // Inverted list information
    protected ITypeTraits[] invListTypeTraits = new ITypeTraits[] { IntegerPointable.TYPE_TRAITS };
    protected IBinaryComparatorFactory[] invListCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(IntegerPointable.FACTORY) };

    public LSMInvertedIndexTestHarness() {
        this.diskPageSize = DEFAULT_DISK_PAGE_SIZE;
        this.diskNumPages = DEFAULT_DISK_NUM_PAGES;
        this.diskMaxOpenFiles = DEFAULT_DISK_MAX_OPEN_FILES;
        this.memPageSize = DEFAULT_MEM_PAGE_SIZE;
        this.memNumPages = DEFAULT_MEM_NUM_PAGES;
        this.hyracksFrameSize = DEFAULT_HYRACKS_FRAME_SIZE;
    }

    public LSMInvertedIndexTestHarness(int diskPageSize, int diskNumPages, int diskMaxOpenFiles, int memPageSize,
            int memNumPages, int hyracksFrameSize) {
        this.diskPageSize = diskPageSize;
        this.diskNumPages = diskNumPages;
        this.diskMaxOpenFiles = diskMaxOpenFiles;
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
        this.hyracksFrameSize = hyracksFrameSize;
    }

    public void setUp() throws HyracksException {
        onDiskDir = "lsm_invertedindex_" + simpleDateFormat.format(new Date()) + sep;
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(diskPageSize, diskNumPages, diskMaxOpenFiles);
        diskBufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        diskFileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        memBufferCache = new InMemoryBufferCache(new HeapBufferAllocator(), memPageSize, memNumPages);
        memFreePageManager = new InMemoryFreePageManager(memNumPages, new LIFOMetaDataFrameFactory());
        ioManager = TestStorageManagerComponentHolder.getIOManager();
        rnd.setSeed(RANDOM_SEED);
        
        File btreeFile = new File(onDiskDir + btreeFileName);
        btreeFile.deleteOnExit();
        File invIndexFile = new File(onDiskDir + invIndexFileName);
        invIndexFile.deleteOnExit();
        btreeFileRef = new FileReference(btreeFile);
        invIndexFileRef = new FileReference(invIndexFile);
        diskBufferCache.createFile(btreeFileRef);
        diskBufferCache.openFile(diskFileMapProvider.lookupFileId(btreeFileRef));
        diskBufferCache.createFile(invIndexFileRef);
        diskBufferCache.openFile(diskFileMapProvider.lookupFileId(invIndexFileRef));
    }

    public void tearDown() throws HyracksDataException {
        diskBufferCache.closeFile(diskFileMapProvider.lookupFileId(btreeFileRef));
        diskBufferCache.deleteFile(diskFileMapProvider.lookupFileId(btreeFileRef), false);
        diskBufferCache.closeFile(diskFileMapProvider.lookupFileId(invIndexFileRef));
        diskBufferCache.deleteFile(diskFileMapProvider.lookupFileId(invIndexFileRef), false);
        diskBufferCache.close();
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            File dir = new File(dev.getPath(), onDiskDir);
            FilenameFilter filter = new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return !name.startsWith(".");
                }
            };
            String[] files = dir.list(filter);
            if (files != null) {
                for (String fileName : files) {
                    File file = new File(dir.getPath() + File.separator + fileName);
                    file.delete();
                }
            }
            dir.delete();
        }
    }
    
    public int getDiskInvertedIndexFileId() throws HyracksDataException {
        return diskFileMapProvider.lookupFileId(invIndexFileRef);
    }
    
    public int getDiskBtreeFileId() throws HyracksDataException {
        return diskFileMapProvider.lookupFileId(btreeFileRef);
    }

    public int getDiskPageSize() {
        return diskPageSize;
    }

    public int getDiskNumPages() {
        return diskNumPages;
    }

    public int getDiskMaxOpenFiles() {
        return diskMaxOpenFiles;
    }

    public int getMemPageSize() {
        return memPageSize;
    }

    public int getMemNumPages() {
        return memNumPages;
    }

    public int getHyracksFrameSize() {
        return hyracksFrameSize;
    }

    public int getFileId() {
        return DUMMY_FILE_ID;
    }

    public IOManager getIOManager() {
        return ioManager;
    }

    public IBufferCache getDiskBufferCache() {
        return diskBufferCache;
    }

    public IFileMapProvider getDiskFileMapProvider() {
        return diskFileMapProvider;
    }

    public InMemoryBufferCache getMemBufferCache() {
        return memBufferCache;
    }

    public InMemoryFreePageManager getMemFreePageManager() {
        return memFreePageManager;
    }

    public IHyracksTaskContext getHyracksTastContext() {
        return ctx;
    }

    public String getOnDiskDir() {
        return onDiskDir;
    }

    public Random getRandom() {
        return rnd;
    }
    
    public ITypeTraits[] getTokenTypeTraits() {
        return tokenTypeTraits;
    }
    
    public ITypeTraits[] getInvertedListTypeTraits() {
        return invListTypeTraits;
    }
    
    public IBinaryComparatorFactory[] getTokenBinaryComparatorFactories() {
        return tokenCmpFactories;
    }
    
    public IBinaryComparatorFactory[] getInvertedListBinaryComparatorFactories() {
        return invListCmpFactories;
    }
}
