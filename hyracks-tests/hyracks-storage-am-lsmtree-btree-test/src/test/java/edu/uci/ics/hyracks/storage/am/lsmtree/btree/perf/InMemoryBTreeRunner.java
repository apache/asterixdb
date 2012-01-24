package edu.uci.ics.hyracks.storage.am.lsmtree.btree.perf;

import java.text.SimpleDateFormat;
import java.util.Date;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleBatch;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;

public class InMemoryBTreeRunner extends Thread implements IExperimentRunner {
    protected IBufferCache bufferCache;
    protected int btreeFileId;
    
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected String fileName;
    
    protected final int numBatches;
    protected BTree btree;    
    
    public InMemoryBTreeRunner(int numBatches, int pageSize, int numPages, ITypeTraits[] typeTraits, MultiComparator cmp) throws HyracksDataException, BTreeException {
        this.numBatches = numBatches;
        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        init(pageSize, numPages, typeTraits, cmp);
    }
    
    protected void init(int pageSize, int numPages, ITypeTraits[] typeTraits, MultiComparator cmp) throws HyracksDataException, BTreeException {
    	ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        bufferCache = new InMemoryBufferCache(allocator, pageSize, numPages);
        // Chose an aribtrary file id.
        btreeFileId = 0;
        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager freePageManager = new InMemoryFreePageManager(bufferCache.getNumPages(), metaFrameFactory);
        btree = new BTree(bufferCache, typeTraits.length, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
    }

    @Override
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception {        
        BTreeThread[] threads = new BTreeThread[numThreads];
        int threadNumBatches = numBatches / numThreads;
    	for (int i = 0; i < numThreads; i++) {
    		threads[i] = new BTreeThread(dataGen, btree, threadNumBatches);
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
    public void init() throws Exception {
    }

    @Override
    public void deinit() throws Exception {
        bufferCache.closeFile(btreeFileId);
        bufferCache.close();
    }

	@Override
	public void reset() throws Exception {
		btree.create(btreeFileId);		
	}
	
	public class BTreeThread extends Thread {
    	private final DataGenThread dataGen;
    	private final int numBatches;
    	private final ITreeIndexAccessor indexAccessor;
    	public BTreeThread(DataGenThread dataGen, BTree btree, int numBatches) {
    		this.dataGen = dataGen;
    		this.numBatches = numBatches;
    		indexAccessor = btree.createAccessor();
    	}
    	
        @Override
        public void run() {
            try {
                for (int i = 0; i < numBatches; i++) {
                    TupleBatch batch = dataGen.tupleBatchQueue.take();
                    for (int j = 0; j < batch.size(); j++) {
                        try {
                        	indexAccessor.insert(batch.get(j));
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
