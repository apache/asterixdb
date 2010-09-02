package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Random;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public abstract class AbstractBTreeOperatorNodePushable extends AbstractUnaryOutputOperatorNodePushable {
		
	protected IBTreeInteriorFrame interiorFrame;
	protected IBTreeLeafFrame leafFrame;
	
	protected BTree btree;
	
	protected AbstractBTreeOperatorDescriptor opDesc;	
	protected IHyracksContext ctx;
	
	public AbstractBTreeOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, final IHyracksContext ctx) {
		this.opDesc = opDesc;
		this.ctx = ctx;
	}
	
	public void init() throws FileNotFoundException {
		IBufferCache bufferCache = opDesc.getBufferCacheProvider().getBufferCache();
		FileManager fileManager = opDesc.getBufferCacheProvider().getFileManager();
		
        File f = new File(opDesc.getBtreeFileName());
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        
        try {
        	FileInfo fi = new FileInfo(opDesc.getBtreeFileId(), raf);
        	fileManager.registerFile(fi);
        }
        catch (Exception e) {
        }
        
		BTreeRegistry btreeRegistry = opDesc.getBtreeRegistryProvider().getBTreeRegistry();
		btree = btreeRegistry.get(opDesc.getBtreeFileId());
        if(btree == null) {
        	
        	// create new btree and register it            
            btreeRegistry.lock();
            try {
                // check if btree has already been registered by another thread
                btree = btreeRegistry.get(opDesc.getBtreeFileId());
                if(btree == null) {
                                    	
                	// this thread should create and register the btee                                        
                    btree = new BTree(bufferCache, 
                    		opDesc.getInteriorFactory(), 
                    		opDesc.getLeafFactory(), 
                    		opDesc.getMultiComparator());
                    btree.open(opDesc.getBtreeFileId());
                    btreeRegistry.register(opDesc.getBtreeFileId(), btree);
                }                
            }
            finally {                        
                btreeRegistry.unlock();
            }
        }            
                
        interiorFrame = opDesc.getInteriorFactory().getFrame();
        leafFrame = opDesc.getLeafFactory().getFrame();    	
	}	
	
	// debug
	protected void fill() throws Exception {
		MetaDataFrame metaFrame = new MetaDataFrame();
		
		btree.create(opDesc.getBtreeFileId(), leafFrame, metaFrame);
		
		Random rnd = new Random();
		rnd.setSeed(50);				
		
		for (int i = 0; i < 10000; i++) {
			ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
			DataOutputStream dos = new DataOutputStream(baaos);        	        	

			int f0 = rnd.nextInt() % 10000;
			int f1 = 5;

			IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
			IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);

			byte[] record = baaos.toByteArray();

			if (i % 1000 == 0) {
				System.out.println("INSERTING " + i + " : " + f0 + " " + f1);            	
			}

			try {                                
				btree.insert(record, leafFrame, interiorFrame, metaFrame);
			} catch (Exception e) {
			}
		}

		/*
        IFieldAccessor[] fields = new IFieldAccessor[2];
        fields[0] = new Int32Accessor(); // key field
        fields[1] = new Int32Accessor(); // value field

        int keyLen = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator cmp = new MultiComparator(cmps, fields);		

        ByteArrayAccessibleOutputStream lkbaaos = new ByteArrayAccessibleOutputStream();
        DataOutputStream lkdos = new DataOutputStream(lkbaaos);    	    	    	
        IntegerSerializerDeserializer.INSTANCE.serialize(-1000, lkdos);

        ByteArrayAccessibleOutputStream hkbaaos = new ByteArrayAccessibleOutputStream();
        DataOutputStream hkdos = new DataOutputStream(hkbaaos);    	    	    	
        IntegerSerializerDeserializer.INSTANCE.serialize(1000, hkdos);

        byte[] lowKey = lkbaaos.toByteArray();
        byte[] highKey = hkbaaos.toByteArray();

        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps, fields);

        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(cursor, rangePred, leafFrame, interiorFrame);
        try {
            while (cursor.hasNext()) {
            	cursor.next();
                byte[] array = cursor.getPage().getBuffer().array();
                int recOffset = cursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
                System.out.println(rec);         
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cursor.close();
        }	
        */	 
	}
}
