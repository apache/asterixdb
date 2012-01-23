package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public abstract class AbstractLSMTreeTest {
    protected static final Logger LOGGER = Logger.getLogger(AbstractLSMTreeTest.class.getName());
    public static final long RANDOM_SEED = 50;
    
    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int MAX_OPEN_FILES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
        
    protected IHyracksTaskContext ctx; 
    protected IBufferCache bufferCache;
    protected int btreeFileId;
    
    protected final Random rnd = new Random();
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected String fileName;    
    
    @Before
    public void setUp() throws HyracksDataException {
        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(getPageSize(), getNumPages(), getMaxOpenFiles());
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName)); 
        bufferCache.createFile(file);
        btreeFileId = fmp.lookupFileId(file);
        bufferCache.openFile(btreeFileId);
        rnd.setSeed(RANDOM_SEED);
    }
    
    @After
    public void tearDown() throws HyracksDataException {
        bufferCache.closeFile(btreeFileId);
        bufferCache.close();
        File f = new File(fileName);
        f.deleteOnExit();
    }
    
    public int getPageSize() {
        return PAGE_SIZE;
    }
    
    public int getNumPages() {
        return NUM_PAGES;
    }
    
    public int getHyracksFrameSize() {
        return HYRACKS_FRAME_SIZE;
    }
    
    public int getMaxOpenFiles() {
        return MAX_OPEN_FILES;
    }
}
