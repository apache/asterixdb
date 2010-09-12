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

package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.DataOutput;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public abstract class AbstractBTreeOperatorNodePushable extends AbstractUnaryOutputOperatorNodePushable {
	
	protected IBTreeInteriorFrame interiorFrame;
	protected IBTreeLeafFrame leafFrame;
	
	protected BTree btree;
	
	protected AbstractBTreeOperatorDescriptor opDesc;	
	protected IHyracksContext ctx;
	
	protected boolean createBTree;
	
	public AbstractBTreeOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, final IHyracksContext ctx, boolean createBTree) {
		this.opDesc = opDesc;
		this.ctx = ctx;
		this.createBTree = createBTree;
	}
	
	public void init() throws Exception {				
		IBufferCache bufferCache = opDesc.getBufferCacheProvider().getBufferCache();
		FileManager fileManager = opDesc.getBufferCacheProvider().getFileManager();
		
        File f = new File(opDesc.getBtreeFileName());
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        
        if(!f.exists() && !createBTree) {
        	throw new Exception("Trying to open btree from file " + opDesc.getBtreeFileName() + " but file doesn't exist.");
        }
        
        try {
        	FileInfo fi = new FileInfo(opDesc.getBtreeFileId(), raf);
        	fileManager.registerFile(fi);
        }
        catch (Exception e) {
        }
        
        interiorFrame = opDesc.getInteriorFactory().getFrame();
        leafFrame = opDesc.getLeafFactory().getFrame();    	
        
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
                	
                	// start by building the multicomparator from the factories
                	IFieldAccessor[] fields = new IFieldAccessor[opDesc.getFieldAccessorFactories().length];
                	for(int i = 0; i < opDesc.getFieldAccessorFactories().length; i++) {
                		fields[i] = opDesc.getFieldAccessorFactories()[i].getFieldAccessor();
                	}
                	
                	IBinaryComparator[] comparators = new IBinaryComparator[opDesc.getComparatorFactories().length];
                	for(int i = 0; i < opDesc.getComparatorFactories().length; i++) {
                		comparators[i] = opDesc.getComparatorFactories()[i].createBinaryComparator();
                	}
                	
                    MultiComparator cmp = new MultiComparator(comparators, fields);
                	
                	btree = new BTree(bufferCache, opDesc.getInteriorFactory(), opDesc.getLeafFactory(), cmp);
                	if(createBTree) {
                		MetaDataFrame metaFrame = new MetaDataFrame();                		
                		btree.create(opDesc.getBtreeFileId(), leafFrame, metaFrame);
                	}
                	btree.open(opDesc.getBtreeFileId());
                    btreeRegistry.register(opDesc.getBtreeFileId(), btree);
                }                
            }
            finally {                        
                btreeRegistry.unlock();
            }
        }                      
	}	
	
	// debug
	protected void fill() throws Exception {
		
		
		// TODO: uncomment and fix
		MetaDataFrame metaFrame = new MetaDataFrame();                		
		btree.create(opDesc.getBtreeFileId(), leafFrame, metaFrame);
		
		Random rnd = new Random();
		rnd.setSeed(50);				
		
		ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);				
		ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
		DataOutput dos = tb.getDataOutput();

		ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE};
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recDesc);
		accessor.reset(frame);
		FrameTupleReference tuple = new FrameTupleReference();
		
		for (int i = 0; i < 10000; i++) {			
			int f0 = rnd.nextInt() % 10000;
			int f1 = 5;

			tb.reset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	tb.addFieldEndOffset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	tb.addFieldEndOffset();        	
        	        	
        	appender.reset(frame, true);
        	appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        	
        	tuple.reset(accessor, 0);
						
			if (i % 1000 == 0) {
				System.out.println("INSERTING " + i + " : " + f0 + " " + f1);            	
			}

			try {                                
				btree.insert(tuple, leafFrame, interiorFrame, metaFrame);
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
	
	protected byte[] buildBTreeRecordFromHyraxRecord(IFrameTupleAccessor accessor, int tupleId, int[] keyFields, int[] payloadFields) {
		
		// determine size of record
		int btreeRecordSize = 0;			
		for(int j = 0; j < keyFields.length; j++) {
			btreeRecordSize += accessor.getFieldLength(tupleId, keyFields[j]);
		}
		for(int j = 0; j < payloadFields.length; j++) {
			btreeRecordSize += accessor.getFieldLength(tupleId, payloadFields[j]);
		}			
		
		// allocate record and copy fields
		byte[] btreeRecord = new byte[btreeRecordSize];
		int recRunner = 0;
		for(int j = 0; j < keyFields.length; j++) {
			int fieldStartOff = accessor.getTupleStartOffset(tupleId) + + accessor.getFieldSlotsLength() + accessor.getFieldStartOffset(tupleId, keyFields[j]);				
			int fieldLength = accessor.getFieldLength(tupleId, keyFields[j]);						
			System.arraycopy(accessor.getBuffer().array(), fieldStartOff, btreeRecord, recRunner, fieldLength);				
			recRunner += fieldLength;
		}
		for(int j = 0; j < payloadFields.length; j++) {
			int fieldStartOff = accessor.getTupleStartOffset(tupleId) + + accessor.getFieldSlotsLength() + accessor.getFieldStartOffset(tupleId, payloadFields[j]);
			int fieldLength = accessor.getFieldLength(tupleId, payloadFields[j]);
			System.arraycopy(accessor.getBuffer().array(), fieldStartOff, btreeRecord, recRunner, fieldLength);
			recRunner += fieldLength;				
		}						
		
		return btreeRecord;
	}	
}
