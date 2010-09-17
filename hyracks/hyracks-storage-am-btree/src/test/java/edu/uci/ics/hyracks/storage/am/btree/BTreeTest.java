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

package edu.uci.ics.hyracks.storage.am.btree;

import java.io.DataOutput;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.control.nc.runtime.HyracksContext;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.DiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

@SuppressWarnings("unchecked")

public class BTreeTest {    
	
    //private static final int PAGE_SIZE = 128;
    //private static final int PAGE_SIZE = 8192;
	private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
    
    // to help with the logger madness
    private void print(String str) {
    	System.out.print(str);
    	
//    	if(GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.FINEST)) {            
//        	GlobalConfig.ASTERIX_LOGGER.finest(str);
//        }
    }       
    
    public class BufferAllocator implements ICacheMemoryAllocator {
        @Override
        public ByteBuffer[] allocate(int pageSize, int numPages) {
            ByteBuffer[] buffers = new ByteBuffer[numPages];
            for (int i = 0; i < numPages; ++i) {
                buffers[i] = ByteBuffer.allocate(pageSize);
            }
            return buffers;
        }
    }
        
    // FIXED-LENGTH KEY TEST
    // create a B-tree with one fixed-length "key" field and one fixed-length "value" field
    // fill B-tree with random values using insertions (not bulk load)
    // perform ordered scan and range search
    @Test
    public void test01() throws Exception {
    	
    	print("FIXED-LENGTH KEY TEST\n");
    	
        FileManager fileManager = new FileManager();
        ICacheMemoryAllocator allocator = new BufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        IBufferCache bufferCache = new BufferCache(allocator, prs, fileManager, PAGE_SIZE, NUM_PAGES);
        
        File f = new File("/tmp/btreetest.bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int fileId = 0;
        FileInfo fi = new FileInfo(fileId, raf);
        fileManager.registerFile(fi);
        
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
        IBTreeMetaDataFrameFactory metaFrameFactory = new MetaDataFrameFactory();
        
        IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
        IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
        IBTreeMetaDataFrame metaFrame = metaFrameFactory.getFrame();        
               
        int fieldCount = 2;
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator cmp = new MultiComparator(2, cmps);
        
        BTree btree = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();                
        
        print("INSERTING INTO TREE\n");
        
        IHyracksContext ctx = new HyracksContext(HYRACKS_FRAME_SIZE);        
        ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);				
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
		DataOutput dos = tb.getDataOutput();
		
		ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE};
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recDesc);
		accessor.reset(frame);
		FrameTupleReference tuple = new FrameTupleReference();
		
		// 10000
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
        	
        	//System.out.println(tuple.getFieldCount() + " " + tuple.getFieldLength(0) + " " + tuple.getFieldLength(1));
        	
            if (i % 1000 == 0) {
                long end = System.currentTimeMillis();
                print("INSERTING " + i + " : " + f0 + " " + f1 + " " + (end - start) + "\n");            	
            }
            
            try {                                
                btree.insert(tuple, leafFrame, interiorFrame, metaFrame);
            } catch (BTreeException e) {            	
            } catch (Exception e) {
            	e.printStackTrace();
            }
            
            //btree.printTree(leafFrame, interiorFrame);
            //System.out.println();
        }
        //btree.printTree(leafFrame, interiorFrame);
        //System.out.println();
        
        print("TOTALSPACE: " + f.length() + "\n");
        
        String stats = btree.printStats();
        print(stats);
        
        long end = System.currentTimeMillis();
        long duration = end - start;
        print("DURATION: " + duration + "\n");
                
        // ordered scan
        
        print("ORDERED SCAN:\n");
        IBTreeCursor scanCursor = new RangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrame, interiorFrame);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();                
                ITupleReference frameTuple = scanCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }
                       
        
        // disk-order scan
        print("DISK-ORDER SCAN:\n");        
        DiskOrderScanCursor diskOrderCursor = new DiskOrderScanCursor(leafFrame);
        btree.diskOrderScan(diskOrderCursor, leafFrame, metaFrame);
        try {
            while (diskOrderCursor.hasNext()) {
                diskOrderCursor.next();
                ITupleReference frameTuple = diskOrderCursor.getTuple();                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            diskOrderCursor.close();
        }
         
        
        // range search in [-1000, 1000]
        print("RANGE SEARCH:\n");        
 
        IBTreeCursor rangeCursor = new RangeSearchCursor(leafFrame);

        // build low and high keys                       
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
		DataOutput kdos = ktb.getDataOutput();
		
		ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE };
		RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
		IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx, keyDesc);
		keyAccessor.reset(frame);
		
		appender.reset(frame, true);
		
		// build and append low key
		ktb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(-1000, kdos);
    	ktb.addFieldEndOffset();    	
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
    	
    	// build and append high key
    	ktb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(1000, kdos);
    	ktb.addFieldEndOffset();
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
        
    	// create tuplereferences for search keys
    	FrameTupleReference lowKey = new FrameTupleReference();
    	lowKey.reset(keyAccessor, 0);
    	
		FrameTupleReference highKey = new FrameTupleReference();
		highKey.reset(keyAccessor, 1);
    	
		
        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(fieldCount, searchCmps);
        
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }  
                
        btree.close();

        bufferCache.close();
        fileManager.close();
        print("\n");
    }
    
    
    // COMPOSITE KEY TEST (NON-UNIQUE B-TREE)
    // create a B-tree with one two fixed-length "key" fields and one fixed-length "value" field
    // fill B-tree with random values using insertions (not bulk load)
    // perform ordered scan and range search
    @Test
    public void test02() throws Exception {

    	print("COMPOSITE KEY TEST\n");
    	
        FileManager fileManager = new FileManager();
        ICacheMemoryAllocator allocator = new BufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        IBufferCache bufferCache = new BufferCache(allocator, prs, fileManager, PAGE_SIZE, NUM_PAGES);

        File f = new File("/tmp/btreetest.bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int fileId = 0;
        FileInfo fi = new FileInfo(fileId, raf);
        fileManager.registerFile(fi);
                
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
        IBTreeMetaDataFrameFactory metaFrameFactory = new MetaDataFrameFactory();
        
        IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
        IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
        IBTreeMetaDataFrame metaFrame = metaFrameFactory.getFrame();   
         
        int fieldCount = 3;
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();       
        MultiComparator cmp = new MultiComparator(fieldCount, cmps);

        BTree btree = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();
        
        print("INSERTING INTO TREE\n");
        
        IHyracksContext ctx = new HyracksContext(HYRACKS_FRAME_SIZE);        
        ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);				
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
		DataOutput dos = tb.getDataOutput();
		
		ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE, 
				IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE};
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recDesc);
		accessor.reset(frame);
		FrameTupleReference tuple = new FrameTupleReference();
        
        for (int i = 0; i < 10000; i++) {        	
        	int f0 = rnd.nextInt() % 2000;
        	int f1 = rnd.nextInt() % 1000;
        	int f2 = 5;
        	        	
        	tb.reset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	tb.addFieldEndOffset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	tb.addFieldEndOffset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
        	tb.addFieldEndOffset();
        	
        	appender.reset(frame, true);
        	appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        	
        	tuple.reset(accessor, 0);
        	        	
            if (i % 1000 == 0) {
            	print("INSERTING " + i + " : " + f0 + " " + f1 + "\n");
            }
            
            try {
                btree.insert(tuple, leafFrame, interiorFrame, metaFrame);
            } catch (Exception e) {
            }
        }
        //btree.printTree(leafFrame, interiorFrame);
        
        long end = System.currentTimeMillis();
        long duration = end - start;
        print("DURATION: " + duration + "\n");        
        
        // try a simple index scan
        print("ORDERED SCAN:\n");        
        IBTreeCursor scanCursor = new RangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrame, interiorFrame);
        
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // range search in [(-3),(3)]
        print("RANGE SEARCH:\n");        
        IBTreeCursor rangeCursor = new RangeSearchCursor(leafFrame);

        
        // build low and high keys             
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
		DataOutput kdos = ktb.getDataOutput();
		
		ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE };
		RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
		IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx, keyDesc);
		keyAccessor.reset(frame);
		
		appender.reset(frame, true);
		
		// build and append low key
		ktb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(-3, kdos);
    	ktb.addFieldEndOffset();    	
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
    	
    	// build and append high key
    	ktb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(3, kdos);
    	ktb.addFieldEndOffset();
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
        
    	// create tuplereferences for search keys
    	FrameTupleReference lowKey = new FrameTupleReference();
    	lowKey.reset(keyAccessor, 0);
    	
		FrameTupleReference highKey = new FrameTupleReference();
		highKey.reset(keyAccessor, 1);
		
		
        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();       
        MultiComparator searchCmp = new MultiComparator(fieldCount, searchCmps); // use only a single comparator for searching
        
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);
        
        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n"); 
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        btree.close();
        
        bufferCache.close();
        fileManager.close();
        
        print("\n");
    }    
    
    
    // VARIABLE-LENGTH TEST
    // create a B-tree with one variable-length "key" field and one variable-length "value" field
    // fill B-tree with random values using insertions (not bulk load)
    // perform ordered scan and range search
    @Test
    public void test03() throws Exception {

    	print("VARIABLE-LENGTH KEY TEST\n");
    	
    	FileManager fileManager = new FileManager();
    	ICacheMemoryAllocator allocator = new BufferAllocator();
    	IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
    	IBufferCache bufferCache = new BufferCache(allocator, prs, fileManager, PAGE_SIZE, NUM_PAGES);

    	File f = new File("/tmp/btreetest.bin");
    	RandomAccessFile raf = new RandomAccessFile(f, "rw");
    	int fileId = 0;
    	FileInfo fi = new FileInfo(fileId, raf);
    	fileManager.registerFile(fi);
    	
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
        IBTreeMetaDataFrameFactory metaFrameFactory = new MetaDataFrameFactory();
        
        IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
        IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
        IBTreeMetaDataFrame metaFrame = metaFrameFactory.getFrame();   
        
    	int fieldCount = 2;
    	int keyFieldCount = 1;
    	IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
    	cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    	MultiComparator cmp = new MultiComparator(fieldCount, cmps);
    	
    	BTree btree = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, cmp);
    	btree.create(fileId, leafFrame, metaFrame);
    	btree.open(fileId);

    	Random rnd = new Random();
    	rnd.setSeed(50);

    	IHyracksContext ctx = new HyracksContext(HYRACKS_FRAME_SIZE);        
        ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);				
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
		DataOutput dos = tb.getDataOutput();
		
		ISerializerDeserializer[] recDescSers = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recDesc);
		accessor.reset(frame);
		FrameTupleReference tuple = new FrameTupleReference();
    	
    	int maxLength = 10; // max string length to be generated
    	for (int i = 0; i < 10000; i++) {
    		String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
    		String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
    		
    		tb.reset();
    		UTF8StringSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	tb.addFieldEndOffset();
        	UTF8StringSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	tb.addFieldEndOffset();        	
        	
        	appender.reset(frame, true);
        	appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        	
        	tuple.reset(accessor, 0);
    		    		
    		if (i % 1000 == 0) {
    			//print("INSERTING " + i + ": " + cmp.printRecord(record, 0) + "\n");
    			print("INSERTING " + i + "\n");
    		}

    		try {
    			btree.insert(tuple, leafFrame, interiorFrame, metaFrame);
    		} catch (Exception e) {
    			//e.printStackTrace();
    		}
    		    		
    	}     
    	// btree.printTree();

    	// ordered scan
        print("ORDERED SCAN:\n");        
        IBTreeCursor scanCursor = new RangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrame, interiorFrame);
        
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");  
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }
        
        // range search in ["cbf", cc7"]
        print("RANGE SEARCH:\n");        
        
        IBTreeCursor rangeCursor = new RangeSearchCursor(leafFrame);
                
        // build low and high keys                       
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
		DataOutput kdos = ktb.getDataOutput();
		
		ISerializerDeserializer[] keyDescSers = { UTF8StringSerializerDeserializer.INSTANCE };
		RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
		IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx, keyDesc);
		keyAccessor.reset(frame);
		
		appender.reset(frame, true);
		
		// build and append low key
		ktb.reset();
		UTF8StringSerializerDeserializer.INSTANCE.serialize("cbf", kdos);
    	ktb.addFieldEndOffset();    	
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
    	
    	// build and append high key
    	ktb.reset();
    	UTF8StringSerializerDeserializer.INSTANCE.serialize("cc7", kdos);
    	ktb.addFieldEndOffset();
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
        
    	// create tuplereferences for search keys
    	FrameTupleReference lowKey = new FrameTupleReference();
    	lowKey.reset(keyAccessor, 0);
    	
		FrameTupleReference highKey = new FrameTupleReference();
		highKey.reset(keyAccessor, 1);
        
		
        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(fieldCount, searchCmps);
        
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");  
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }
    	
        btree.close();

        bufferCache.close();
        fileManager.close();
        
        print("\n");
    }
        
    
    // DELETION TEST
    // create a B-tree with one variable-length "key" field and one variable-length "value" field
    // fill B-tree with random values using insertions, then delete entries one-by-one
    // repeat procedure a few times on same B-tree 
    @Test
    public void test04() throws Exception {

    	print("DELETION TEST\n");
    	
        FileManager fileManager = new FileManager();
        ICacheMemoryAllocator allocator = new BufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        IBufferCache bufferCache = new BufferCache(allocator, prs, fileManager, PAGE_SIZE, NUM_PAGES);

        File f = new File("/tmp/btreetest.bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int fileId = 0;
        FileInfo fi = new FileInfo(fileId, raf);
        fileManager.registerFile(fi);
                
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
        IBTreeMetaDataFrameFactory metaFrameFactory = new MetaDataFrameFactory();
        
        IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
        IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
        IBTreeMetaDataFrame metaFrame = metaFrameFactory.getFrame();   
        
        int fieldCount = 2;
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        
        MultiComparator cmp = new MultiComparator(fieldCount, cmps);

        BTree btree = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        IHyracksContext ctx = new HyracksContext(HYRACKS_FRAME_SIZE);        
        ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);				
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
		DataOutput dos = tb.getDataOutput();
		
		ISerializerDeserializer[] recDescSers = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recDesc);
		accessor.reset(frame);
		FrameTupleReference tuple = new FrameTupleReference();
        
        int runs = 3;
        for (int run = 0; run < runs; run++) {
        	
            print("DELETION TEST RUN: " + (run+1) + "/" + runs + "\n");
            
            print("INSERTING INTO BTREE\n");
            int maxLength = 10;
            int ins = 10000;
            String[] f0s = new String[ins];
            String[] f1s = new String[ins];                                     
            int insDone = 0;
            int[] insDoneCmp = new int[ins];
            for (int i = 0; i < ins; i++) {
                String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                
                f0s[i] = f0;
                f1s[i] = f1;
                
        		tb.reset();
        		UTF8StringSerializerDeserializer.INSTANCE.serialize(f0, dos);
            	tb.addFieldEndOffset();
            	UTF8StringSerializerDeserializer.INSTANCE.serialize(f1, dos);
            	tb.addFieldEndOffset();        	
            	
            	appender.reset(frame, true);
            	appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            	
            	tuple.reset(accessor, 0);                        		                            	            	

                if (i % 1000 == 0) {
                	print("INSERTING " + i + "\n");
                	//print("INSERTING " + i + ": " + cmp.printRecord(record, 0) + "\n");                                       
                }

                try {
                    btree.insert(tuple, leafFrame, interiorFrame, metaFrame);
                    insDone++;
                } catch (BTreeException e) {                	
                } catch (Exception e) {
                	e.printStackTrace();
                }

                insDoneCmp[i] = insDone;
            }
            // btree.printTree();
            // btree.printStats();

            print("DELETING FROM BTREE\n");
            int delDone = 0;
            for (int i = 0; i < ins; i++) {
            	
            	tb.reset();
        		UTF8StringSerializerDeserializer.INSTANCE.serialize(f0s[i], dos);
            	tb.addFieldEndOffset();
            	UTF8StringSerializerDeserializer.INSTANCE.serialize(f1s[i], dos);
            	tb.addFieldEndOffset();        	
            	
            	appender.reset(frame, true);
            	appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            	
            	tuple.reset(accessor, 0);    
            	
                if (i % 1000 == 0) {
                    //print("DELETING " + i + ": " + cmp.printRecord(records[i], 0) + "\n");
                	print("DELETING " + i + "\n");
                }

                try {
                    btree.delete(tuple, leafFrame, interiorFrame, metaFrame);
                    delDone++;
                } catch (BTreeException e) {                	
                } catch (Exception e) {
                	e.printStackTrace();
                }

                if (insDoneCmp[i] != delDone) {
                    print("INCONSISTENT STATE, ERROR IN DELETION TEST\n");
                    print("INSDONECMP: " + insDoneCmp[i] + " " + delDone + "\n"); 
                    break;
                }                     
                // btree.printTree();
            }
            //btree.printTree(leafFrame, interiorFrame);
            
            if (insDone != delDone) {
            	print("ERROR! INSDONE: " + insDone + " DELDONE: " + delDone);
            	break;
            }            
        }
        
        btree.close();

        bufferCache.close();
        fileManager.close();
        
        print("\n");
    }
        
    
    // BULK LOAD TEST
    // insert 100,000 records in bulk
    // B-tree has a composite key to "simulate" non-unique index creation
    // do range search
    @Test
    public void test05() throws Exception {

    	print("BULK LOAD TEST\n");
    	
        FileManager fileManager = new FileManager();
        ICacheMemoryAllocator allocator = new BufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        IBufferCache bufferCache = new BufferCache(allocator, prs, fileManager, PAGE_SIZE, NUM_PAGES);

        File f = new File("/tmp/btreetest.bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int fileId = 0;
        FileInfo fi = new FileInfo(fileId, raf);
        fileManager.registerFile(fi);
        
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
        IBTreeMetaDataFrameFactory metaFrameFactory = new MetaDataFrameFactory();
        
        IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
        IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
        IBTreeMetaDataFrame metaFrame = metaFrameFactory.getFrame();       
        
        int fieldCount = 3;
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        
        MultiComparator cmp = new MultiComparator(fieldCount, cmps);

        BTree btree = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        IHyracksContext ctx = new HyracksContext(HYRACKS_FRAME_SIZE);        
        ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);				
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
		DataOutput dos = tb.getDataOutput();
		
		ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recDesc);
		accessor.reset(frame);
		FrameTupleReference tuple = new FrameTupleReference();
        
		BTree.BulkLoadContext bulkLoadCtx = btree.beginBulkLoad(0.7f, leafFrame, interiorFrame, metaFrame);
		
        // generate sorted records
        int ins = 100000;
        print("BULK LOADING " + ins + " RECORDS\n");
        long start = System.currentTimeMillis();
        for (int i = 0; i < ins; i++) {
        	
        	tb.reset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(i, dos);
        	tb.addFieldEndOffset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(i, dos);
        	tb.addFieldEndOffset();        	
        	IntegerSerializerDeserializer.INSTANCE.serialize(5, dos);
        	tb.addFieldEndOffset();
        	        	
        	appender.reset(frame, true);
        	appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        	
        	tuple.reset(accessor, 0);
        	
        	btree.bulkLoadAddTuple(bulkLoadCtx, tuple);        	
        }

        btree.endBulkLoad(bulkLoadCtx);
        
        //btree.printTree(leafFrame, interiorFrame);
        
        long end = System.currentTimeMillis();
        long duration = end - start;
        print("DURATION: " + duration + "\n");
        
        // range search
        print("RANGE SEARCH:\n");
        IBTreeCursor rangeCursor = new RangeSearchCursor(leafFrame);
        
        // build low and high keys                       
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(1);
		DataOutput kdos = ktb.getDataOutput();
		
		ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE };
		RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
		IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx, keyDesc);
		keyAccessor.reset(frame);
		
		appender.reset(frame, true);
		
		// build and append low key
		ktb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(44444, kdos);
    	ktb.addFieldEndOffset();    	
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
    	
    	// build and append high key
    	ktb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(44500, kdos);
    	ktb.addFieldEndOffset();
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
        
    	// create tuplereferences for search keys
    	FrameTupleReference lowKey = new FrameTupleReference();
    	lowKey.reset(keyAccessor, 0);
    	
		FrameTupleReference highKey = new FrameTupleReference();
		highKey.reset(keyAccessor, 1);
		              
    	        
        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(fieldCount, searchCmps);
        
        // TODO: check when searching backwards
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);
        
        try {
            while (rangeCursor.hasNext()) {
            	rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        btree.close();

        bufferCache.close();
        fileManager.close();
        
        print("\n");
    }    
        
    // TIME-INTERVAL INTERSECTION DEMO FOR EVENT PEOPLE
    // demo for Arjun to show easy support of intersection queries on time-intervals
    @Test
    public void test06() throws Exception {

    	print("TIME-INTERVAL INTERSECTION DEMO\n");
    	
        FileManager fileManager = new FileManager();
        ICacheMemoryAllocator allocator = new BufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        IBufferCache bufferCache = new BufferCache(allocator, prs, fileManager, PAGE_SIZE, NUM_PAGES);

        File f = new File("/tmp/btreetest.bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int fileId = 0;
        FileInfo fi = new FileInfo(fileId, raf);
        fileManager.registerFile(fi);       
                
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
        IBTreeMetaDataFrameFactory metaFrameFactory = new MetaDataFrameFactory();
        
        IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
        IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
        IBTreeMetaDataFrame metaFrame = metaFrameFactory.getFrame();
        
        int fieldCount = 3;
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();        
        MultiComparator cmp = new MultiComparator(fieldCount, cmps);

        BTree btree = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);
        
        IHyracksContext ctx = new HyracksContext(HYRACKS_FRAME_SIZE);        
        ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);				
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
		DataOutput dos = tb.getDataOutput();
		
		ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recDesc);
		accessor.reset(frame);
		FrameTupleReference tuple = new FrameTupleReference();
        
        long start = System.currentTimeMillis();
        
        int intervalCount = 10;
        int[][] intervals = new int[intervalCount][2];

        intervals[0][0] = 10;
        intervals[0][1] = 20;

        intervals[1][0] = 11;
        intervals[1][1] = 20;

        intervals[2][0] = 12;
        intervals[2][1] = 20;

        intervals[3][0] = 13;
        intervals[3][1] = 20;

        intervals[4][0] = 14;
        intervals[4][1] = 20;

        intervals[5][0] = 20;
        intervals[5][1] = 30;

        intervals[6][0] = 20;
        intervals[6][1] = 31;

        intervals[7][0] = 20;
        intervals[7][1] = 32;

        intervals[8][0] = 20;
        intervals[8][1] = 33;

        intervals[9][0] = 20;
        intervals[9][1] = 35;

        // int exceptionCount = 0;
        for (int i = 0; i < intervalCount; i++) {        	
        	int f0 = intervals[i][0];
        	int f1 = intervals[i][1];
        	int f2 = rnd.nextInt() % 100;
        	
        	tb.reset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	tb.addFieldEndOffset();
        	IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	tb.addFieldEndOffset();        	
        	IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
        	tb.addFieldEndOffset();
        	
        	appender.reset(frame, true);
        	appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        	
        	tuple.reset(accessor, 0);
        	        	
            //print("INSERTING " + i + " : " + f0 + " " + f1 + "\n");
        	print("INSERTING " + i + "\n");

            try {
                btree.insert(tuple, leafFrame, interiorFrame, metaFrame);
            } catch (Exception e) {
                // e.printStackTrace();
            }
        }
        // btree.printTree(leafFrame, interiorFrame);
        // btree.printStats();

        long end = System.currentTimeMillis();
        long duration = end - start;
        print("DURATION: " + duration + "\n");

        // try a simple index scan

        print("ORDERED SCAN:\n");
        IBTreeCursor scanCursor = new RangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrame, interiorFrame);

        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // try a range search
        print("RANGE SEARCH:\n");
        IBTreeCursor rangeCursor = new RangeSearchCursor(leafFrame);

        // build low and high keys                       
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
		DataOutput kdos = ktb.getDataOutput();
		
		ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
		RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
		IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx, keyDesc);
		keyAccessor.reset(frame);
		
		appender.reset(frame, true);
		
		// build and append low key
		ktb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(12, kdos);
    	ktb.addFieldEndOffset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(12, kdos);
    	ktb.addFieldEndOffset();    	
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
    	
    	// build and append high key
    	ktb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(19, kdos);
    	ktb.addFieldEndOffset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(19, kdos);
    	ktb.addFieldEndOffset();
    	appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());
        
    	// create tuplereferences for search keys
    	FrameTupleReference lowKey = new FrameTupleReference();
    	lowKey.reset(keyAccessor, 0);
    	
		FrameTupleReference highKey = new FrameTupleReference();
		highKey.reset(keyAccessor, 1);
		               
        
        IBinaryComparator[] searchCmps = new IBinaryComparator[2];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        searchCmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(fieldCount, searchCmps);
                
        //print("INDEX RANGE SEARCH ON: " + cmp.printKey(lowKey, 0) + " " + cmp.printKey(highKey, 0) + "\n");                
        
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);
        
        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        btree.close();

        bufferCache.close();
        fileManager.close();
        
        print("\n");
    }
    
    public static String randomString(int length, Random random) {
        String s = Long.toHexString(Double.doubleToLongBits(random.nextDouble()));
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < s.length() && i < length; i++) {
            strBuilder.append(s.charAt(Math.abs(random.nextInt()) % s.length()));
        }
        return strBuilder.toString();
    }    
}