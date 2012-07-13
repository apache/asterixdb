package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class InvertedIndexTestHarness {
    private static final long RANDOM_SEED = 50;

    private final int pageSize;
    private final int numPages;
    private final int maxOpenFiles;
    private final int hyracksFrameSize;

    private IHyracksTaskContext ctx;
    private IBufferCache bufferCache;
    private IFileMapProvider fileMapProvider;
    private FileReference file;

    private final Random rnd = new Random();
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    private final String tmpDir = System.getProperty("java.io.tmpdir");
    private final String sep = System.getProperty("file.separator");
    private String fileName;

    public InvertedIndexTestHarness() {
        this(AccessMethodTestsConfig.INVINDEX_PAGE_SIZE, AccessMethodTestsConfig.INVINDEX_NUM_PAGES,
                AccessMethodTestsConfig.INVINDEX_MAX_OPEN_FILES, AccessMethodTestsConfig.INVINDEX_HYRACKS_FRAME_SIZE);
    }

    public InvertedIndexTestHarness(int pageSize, int numPages, int maxOpenFiles, int hyracksFrameSize) {
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.maxOpenFiles = maxOpenFiles;
        this.hyracksFrameSize = hyracksFrameSize;
    }

    public void setUp() throws HyracksDataException {
        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(pageSize, numPages, maxOpenFiles);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        fileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        file = new FileReference(new File(fileName));
        rnd.setSeed(RANDOM_SEED);
    }

    public void tearDown() throws HyracksDataException {
        bufferCache.close();
        file.delete();
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public IFileMapProvider getFileMapProvider() {
        return fileMapProvider;
    }

    public FileReference getFileReference() {
        return file;
    }

    public String getFileName() {
        return fileName;
    }

    public Random getRandom() {
        return rnd;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getNumPages() {
        return numPages;
    }

    public int getHyracksFrameSize() {
        return hyracksFrameSize;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }
}
