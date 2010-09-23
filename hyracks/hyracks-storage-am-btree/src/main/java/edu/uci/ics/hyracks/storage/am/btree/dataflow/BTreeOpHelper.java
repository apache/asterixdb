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
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

final class BTreeOpHelper {
    private IBTreeInteriorFrame interiorFrame;
    private IBTreeLeafFrame leafFrame;

    private BTree btree;

    private AbstractBTreeOperatorDescriptor opDesc;
    private IHyracksContext ctx;

    private boolean createBTree;

    private boolean isLocalCluster;
    
    BTreeOpHelper(AbstractBTreeOperatorDescriptor opDesc, final IHyracksContext ctx, boolean createBTree, boolean isLocalCluster) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.createBTree = createBTree;
        this.isLocalCluster = isLocalCluster;
    }  
    
    void init() throws Exception {
    	
    	IBufferCache bufferCache = opDesc.getBufferCacheProvider().getBufferCache();
        FileManager fileManager = opDesc.getBufferCacheProvider().getFileManager();
                                
        String fileName = opDesc.getBtreeFileName();
        if(isLocalCluster) {
        	String s = bufferCache.toString();
            String[] splits = s.split("\\.");
        	String bufferCacheAddr = splits[splits.length-1].replaceAll("BufferCache@", "");
        	fileName = fileName + bufferCacheAddr;
        }
        
        File f = new File(fileName);        
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        
        if (!f.exists() && !createBTree) {
            throw new Exception("Trying to open btree from file " + opDesc.getBtreeFileName() + " but file doesn't exist.");
        }
        
        try {
            FileInfo fi = new FileInfo(opDesc.getBtreeFileId(), raf);
            fileManager.registerFile(fi);
        } catch (Exception e) {
        }
        
        interiorFrame = opDesc.getInteriorFactory().getFrame();
        leafFrame = opDesc.getLeafFactory().getFrame();

        BTreeRegistry btreeRegistry = opDesc.getBtreeRegistryProvider().getBTreeRegistry();
        btree = btreeRegistry.get(opDesc.getBtreeFileId());
        if (btree == null) {
        	
            // create new btree and register it            
            btreeRegistry.lock();
            try {
                // check if btree has already been registered by another thread
                btree = btreeRegistry.get(opDesc.getBtreeFileId());
                if (btree == null) {
                    // this thread should create and register the btee
                	                   
                    IBinaryComparator[] comparators = new IBinaryComparator[opDesc.getComparatorFactories().length];
                    for (int i = 0; i < opDesc.getComparatorFactories().length; i++) {
                        comparators[i] = opDesc.getComparatorFactories()[i].createBinaryComparator();
                    }

                    MultiComparator cmp = new MultiComparator(opDesc.getFieldCount(), comparators);
                    
                    btree = new BTree(bufferCache, opDesc.getInteriorFactory(), opDesc.getLeafFactory(), cmp);
                    if (createBTree) {
                        MetaDataFrame metaFrame = new MetaDataFrame();
                        btree.create(opDesc.getBtreeFileId(), leafFrame, metaFrame);
                    }
                    btree.open(opDesc.getBtreeFileId());
                    btreeRegistry.register(opDesc.getBtreeFileId(), btree);
                }
            } finally {
                btreeRegistry.unlock();
            }
        }
    }

    // debug
    void fill() throws Exception {

        // TODO: uncomment and fix
        MetaDataFrame metaFrame = new MetaDataFrame();
        btree.create(opDesc.getBtreeFileId(), leafFrame, metaFrame);

        Random rnd = new Random();
        rnd.setSeed(50);

        ByteBuffer frame = ctx.getResourceManager().allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
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

    public BTree getBTree() {
        return btree;
    }

    public IHyracksContext getHyracksContext() {
        return ctx;
    }

    public AbstractBTreeOperatorDescriptor getOperatorDescriptor() {
        return opDesc;
    }

    public IBTreeLeafFrame getLeafFrame() {
        return leafFrame;
    }

    public IBTreeInteriorFrame getInteriorFrame() {
        return interiorFrame;
    }
}