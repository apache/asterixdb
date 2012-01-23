package edu.uci.ics.hyracks.storage.am.lsmtree.perf;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.lsmtree.datagen.TupleBatch;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.InMemoryBufferCacheFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.LSMTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class LSMTreeRunner implements IExperimentRunner {

    private static final int MAX_OPEN_FILES = 10000;
    private static final int HYRACKS_FRAME_SIZE = 128;
    
    protected IHyracksTaskContext ctx; 
    protected IBufferCache bufferCache;
    protected int lsmtreeFileId;
    
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected final static String classDir = "/lsmtree/";
    protected String fileName;
    
    protected final int numBatches;
    protected final LSMTree lsmtree;
    protected IBufferCache memBufferCache;
    private final int onDiskPageSize;
    private final int onDiskNumPages;
    
    public LSMTreeRunner(int numBatches, int inMemPageSize, int inMeNumPages, int onDiskPageSize, int onDiskNumPages, ITypeTraits[] typeTraits, MultiComparator cmp) throws HyracksDataException, BTreeException {
        this.numBatches = numBatches;
        
        this.onDiskPageSize = onDiskPageSize;
        this.onDiskNumPages = onDiskNumPages;
        
        fileName = tmpDir + classDir + sep + simpleDateFormat.format(new Date());
        ctx = TestUtils.create(HYRACKS_FRAME_SIZE);
        
        
        TestStorageManagerComponentHolder.init(this.onDiskPageSize, this.onDiskNumPages, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        lsmtreeFileId = fmp.lookupFileId(file);
        bufferCache.openFile(lsmtreeFileId);
        
        
        // In Memory
		InMemoryBufferCacheFactory InMemBufferCacheFactory = new InMemoryBufferCacheFactory(inMemPageSize, inMeNumPages);
		memBufferCache = InMemBufferCacheFactory.createInMemoryBufferCache();
		
        lsmtree = LSMTreeUtils.createLSMTree(memBufferCache, bufferCache, lsmtreeFileId, typeTraits, cmp.getComparators(), BTreeLeafFrameType.REGULAR_NSM, (IFileMapManager)fmp);
    }
	@Override
	public void init() throws Exception {
	}

	@Override
	public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception {
	    LSMTreeThread[] threads = new LSMTreeThread[numThreads];
        int threadNumBatches = numBatches / numThreads;
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new LSMTreeThread(dataGen, lsmtree, threadNumBatches);
        }
        // Wait until the tupleBatchQueue is completely full.
        while (dataGen.tupleBatchQueue.remainingCapacity() != 0) {
            Thread.sleep(10);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        long end = System.currentTimeMillis();
        long time = end - start;
        return time;
	}

	@Override
	public void reset() throws Exception {
	    lsmtree.create(lsmtreeFileId);
	}
	
	@Override
	public void deinit() throws Exception {
        bufferCache.closeFile(lsmtreeFileId);
        bufferCache.close();
        memBufferCache.closeFile(lsmtreeFileId);
        memBufferCache.close();
	}

	public class LSMTreeThread extends Thread {
        private final DataGenThread dataGen;
        private final LSMTree lsmTree;
        private final int numBatches;
        private final ITreeIndexAccessor lsmTreeAccessor;
        public LSMTreeThread(DataGenThread dataGen, LSMTree lsmTree, int numBatches) {
            this.dataGen = dataGen;
            this.lsmTree = lsmTree;
            this.numBatches = numBatches;
            lsmTreeAccessor = lsmTree.createAccessor();
        }
        
        @Override
        public void run() {
            try {
                for (int i = 0; i < numBatches; i++) {
                    TupleBatch batch = dataGen.tupleBatchQueue.take();
                    for (int j = 0; j < batch.size(); j++) {
                        try {
                            lsmTreeAccessor.insert(batch.get(j));
                        } catch (TreeIndexException e) {
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
	
}
