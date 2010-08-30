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

import java.io.DataOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeMetaFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeNSMInteriorFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeNSMLeafFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.OrderedSlotManagerFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameInterior;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameInteriorFactory;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameLeafFactory;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameMeta;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameMetaFactory;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.ISlotManagerFactory;
import edu.uci.ics.hyracks.storage.am.btree.types.Int32Accessor;
import edu.uci.ics.hyracks.storage.am.btree.types.StringAccessor;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public class BTreeTest {    
    private static final int PAGE_SIZE = 128;
    // private static final int PAGE_SIZE = 8192;
    private static final int NUM_PAGES = 10;
    
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
        
        ISlotManagerFactory leafSlotManagerFactory = new OrderedSlotManagerFactory();
        ISlotManagerFactory interiorSlotManagerFactory = new OrderedSlotManagerFactory();
        IBTreeFrameLeafFactory leafFrameFactory = new BTreeNSMLeafFactory();
        IBTreeFrameInteriorFactory interiorFrameFactory = new BTreeNSMInteriorFactory();
        IBTreeFrameMetaFactory metaFrameFactory = new BTreeMetaFactory();
        
        IBTreeFrameLeaf leafFrame = leafFrameFactory.getFrame();
        IBTreeFrameInterior interiorFrame = interiorFrameFactory.getFrame();
        IBTreeFrameMeta metaFrame = metaFrameFactory.getFrame();        
        
        IFieldAccessor[] fields = new IFieldAccessor[2];
        fields[0] = new Int32Accessor(); // key field
        fields[1] = new Int32Accessor(); // value field

        int keyLen = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator cmp = new MultiComparator(cmps, fields);

        BTree btree = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, interiorSlotManagerFactory, leafSlotManagerFactory, cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();                
        
        print("INSERTING INTO TREE\n");
        
        for (int i = 0; i < 10000; i++) {
        	ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        	DataOutputStream dos = new DataOutputStream(baaos);        	        	
        	
        	int f0 = rnd.nextInt() % 10000;
        	int f1 = 5;
        	
        	IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	
        	byte[] record = baaos.toByteArray();
        	        	           
            if (i % 1000 == 0) {
                long end = System.currentTimeMillis();
                print("INSERTING " + i + " : " + f0 + " " + f1 + " " + (end - start) + "\n");            	
            }
            
            try {                                
                btree.insert(record, leafFrame, interiorFrame, metaFrame);
            } catch (Exception e) {
            }
        }
        //btree.printTree(leafFrame, interiorFrame);
        
        print("TOTALSPACE: " + f.length() + "\n");
        
        String stats = btree.printStats();
        print(stats);
        
        long end = System.currentTimeMillis();
        long duration = end - start;
        print("DURATION: " + duration + "\n");
        
        // ordered scan
        print("ORDERED SCAN:\n");
        IBTreeCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrame, interiorFrame);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                byte[] array = scanCursor.getPage().getBuffer().array();
                int recOffset = scanCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }                
        
        // disk-order scan
        print("DISK-ORDER SCAN:\n");        
        BTreeDiskOrderScanCursor diskOrderCursor = new BTreeDiskOrderScanCursor(leafFrame);
        btree.diskOrderScan(diskOrderCursor, leafFrame, metaFrame);
        try {
            while (diskOrderCursor.hasNext()) {
                diskOrderCursor.next();
                byte[] array = diskOrderCursor.getPage().getBuffer().array();
                int recOffset = diskOrderCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            diskOrderCursor.close();
        }
               
        // range search in [-1000, 1000]
        print("RANGE SEARCH:\n");        
 
        IBTreeCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);

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
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                byte[] array = rangeCursor.getPage().getBuffer().array();
                int recOffset = rangeCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
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
        
        ISlotManagerFactory leafSlotManagerFactory = new OrderedSlotManagerFactory();
        ISlotManagerFactory interiorSlotManagerFactory = new OrderedSlotManagerFactory();
        IBTreeFrameLeafFactory leafFrameFactory = new BTreeNSMLeafFactory();
        IBTreeFrameInteriorFactory interiorFrameFactory = new BTreeNSMInteriorFactory();
        IBTreeFrameMetaFactory metaFrameFactory = new BTreeMetaFactory();
        
        IBTreeFrameLeaf leafFrame = leafFrameFactory.getFrame();
        IBTreeFrameInterior interiorFrame = interiorFrameFactory.getFrame();
        IBTreeFrameMeta metaFrame = metaFrameFactory.getFrame();   
        
        IFieldAccessor[] fields = new IFieldAccessor[3];
        fields[0] = new Int32Accessor(); // key field 1
        fields[1] = new Int32Accessor(); // key field 2
        fields[2] = new Int32Accessor(); // value field

        int keyLen = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();       
        MultiComparator cmp = new MultiComparator(cmps, fields);

        BTree btree = new BTree(bufferCache, 
        		interiorFrameFactory, 
        		leafFrameFactory, 
        		interiorSlotManagerFactory,
                leafSlotManagerFactory, 
                cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();
        
        print("INSERTING INTO TREE\n");
        for (int i = 0; i < 10000; i++) {
        	ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        	DataOutputStream dos = new DataOutputStream(baaos);        	        	
        	
        	int f0 = rnd.nextInt() % 2000;
        	int f1 = rnd.nextInt() % 1000;
        	int f2 = 5;
        	        	
        	IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
        	
        	byte[] record = baaos.toByteArray();
        	
            if (i % 1000 == 0) {
            	print("INSERTING " + i + " : " + f0 + " " + f1 + "\n");
            }
            
            try {
                btree.insert(record, leafFrame, interiorFrame, metaFrame);
            } catch (Exception e) {
            }
        }
        //btree.printTree(leafFrame, interiorFrame);
        
        long end = System.currentTimeMillis();
        long duration = end - start;
        print("DURATION: " + duration + "\n");        
        
        // try a simple index scan
        print("ORDERED SCAN:\n");        
        IBTreeCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrame, interiorFrame);
        
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                byte[] array = scanCursor.getPage().getBuffer().array();
                int recOffset = scanCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
                print(rec + "\n");                
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // range search in [(-3),(3)]
        print("RANGE SEARCH:\n");        
        IBTreeCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);

        ByteArrayAccessibleOutputStream lkbaaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream lkdos = new DataOutputStream(lkbaaos);    	    	    	
    	IntegerSerializerDeserializer.INSTANCE.serialize(-3, lkdos);
    	
    	ByteArrayAccessibleOutputStream hkbaaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream hkdos = new DataOutputStream(hkbaaos);    	    	    	
    	IntegerSerializerDeserializer.INSTANCE.serialize(3, hkdos);
    	    	        
        byte[] lowKey = lkbaaos.toByteArray();
        byte[] highKey = hkbaaos.toByteArray();
        
        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();       
        MultiComparator searchCmp = new MultiComparator(searchCmps, fields); // use only a single comparator for searching
        
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);
        
        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                byte[] array = rangeCursor.getPage().getBuffer().array();
                int recOffset = rangeCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
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

    	ISlotManagerFactory leafSlotManagerFactory = new OrderedSlotManagerFactory();
        ISlotManagerFactory interiorSlotManagerFactory = new OrderedSlotManagerFactory();
        IBTreeFrameLeafFactory leafFrameFactory = new BTreeNSMLeafFactory();
        IBTreeFrameInteriorFactory interiorFrameFactory = new BTreeNSMInteriorFactory();
        IBTreeFrameMetaFactory metaFrameFactory = new BTreeMetaFactory();
        
        IBTreeFrameLeaf leafFrame = leafFrameFactory.getFrame();
        IBTreeFrameInterior interiorFrame = interiorFrameFactory.getFrame();
        IBTreeFrameMeta metaFrame = metaFrameFactory.getFrame();   

    	IFieldAccessor[] fields = new IFieldAccessor[2];
    	fields[0] = new StringAccessor(); // key        
    	fields[1] = new StringAccessor(); // value

    	int keyLen = 1;
    	IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
    	cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    	MultiComparator cmp = new MultiComparator(cmps, fields);
    	
    	BTree btree = new BTree(bufferCache, 
    			interiorFrameFactory, 
    			leafFrameFactory, 
    			interiorSlotManagerFactory,
    			leafSlotManagerFactory, 
    			cmp);
    	btree.create(fileId, leafFrame, metaFrame);
    	btree.open(fileId);

    	Random rnd = new Random();
    	rnd.setSeed(50);

    	int maxLength = 10; // max string length to be generated
    	for (int i = 0; i < 10000; i++) {
    		String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
    		String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
    		
    		ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        	DataOutputStream dos = new DataOutputStream(baaos);        	        	
        	
        	UTF8StringSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	UTF8StringSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	
    		byte[] record = baaos.toByteArray();
    		
    		if (i % 1000 == 0) {
    			print("INSERTING " + i + ": " + cmp.printRecord(record, 0) + "\n");
    		}

    		try {
    			btree.insert(record, leafFrame, interiorFrame, metaFrame);
    		} catch (Exception e) {
    		}
    	}     
    	// btree.printTree();

    	// ordered scan
        print("ORDERED SCAN:\n");        
        IBTreeCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrame, interiorFrame);
        
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                byte[] array = scanCursor.getPage().getBuffer().array();
                int recOffset = scanCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
                print(rec + "\n");                
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }
        
        // range search in ["cbf", cc7"]
        print("RANGE SEARCH:\n");        
        
        IBTreeCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);
                
        ByteArrayAccessibleOutputStream lkbaaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream lkdos = new DataOutputStream(lkbaaos);    	    	    	
    	UTF8StringSerializerDeserializer.INSTANCE.serialize("cbf", lkdos);
    	
    	ByteArrayAccessibleOutputStream hkbaaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream hkdos = new DataOutputStream(hkbaaos);    	    	    	
    	UTF8StringSerializerDeserializer.INSTANCE.serialize("cc7", hkdos);
        
        byte[] lowKey = lkbaaos.toByteArray();                        
        byte[] highKey = hkbaaos.toByteArray();
        
        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps, fields);
        
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                byte[] array = rangeCursor.getPage().getBuffer().array();
                int recOffset = rangeCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
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
        
        ISlotManagerFactory leafSlotManagerFactory = new OrderedSlotManagerFactory();
        ISlotManagerFactory interiorSlotManagerFactory = new OrderedSlotManagerFactory();
        IBTreeFrameLeafFactory leafFrameFactory = new BTreeNSMLeafFactory();
        IBTreeFrameInteriorFactory interiorFrameFactory = new BTreeNSMInteriorFactory();
        IBTreeFrameMetaFactory metaFrameFactory = new BTreeMetaFactory();
        
        IBTreeFrameLeaf leafFrame = leafFrameFactory.getFrame();
        IBTreeFrameInterior interiorFrame = interiorFrameFactory.getFrame();
        IBTreeFrameMeta metaFrame = metaFrameFactory.getFrame();   
        
        IFieldAccessor[] fields = new IFieldAccessor[2];
        fields[0] = new StringAccessor(); // key        
        fields[1] = new StringAccessor(); // value

        int keyLen = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        
        MultiComparator cmp = new MultiComparator(cmps, fields);

        BTree btree = new BTree(bufferCache, 
        		interiorFrameFactory, 
        		leafFrameFactory, 
        		interiorSlotManagerFactory,
                leafSlotManagerFactory, 
                cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        int runs = 3;
        for (int run = 0; run < runs; run++) {
        	
            print("DELETION TEST RUN: " + (run+1) + "/" + runs + "\n");
            
            print("INSERTING INTO BTREE\n");
            int maxLength = 10;
            int ins = 10000;
            byte[][] records = new byte[ins][];
            int insDone = 0;
            int[] insDoneCmp = new int[ins];
            for (int i = 0; i < ins; i++) {
                String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                
        		ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
            	DataOutputStream dos = new DataOutputStream(baaos);        	        	
            	
            	UTF8StringSerializerDeserializer.INSTANCE.serialize(f0, dos);
            	UTF8StringSerializerDeserializer.INSTANCE.serialize(f1, dos);
                
            	byte[] record = baaos.toByteArray();            	
                records[i] = record;

                if (i % 1000 == 0) {
                    print("INSERTING " + i + ": " + cmp.printRecord(record, 0) + "\n");                                       
                }

                try {
                    btree.insert(record, leafFrame, interiorFrame, metaFrame);
                    insDone++;
                } catch (Exception e) {
                }

                insDoneCmp[i] = insDone;
            }
            // btree.printTree();
            // btree.printStats();

            print("DELETING FROM BTREE\n");
            int delDone = 0;
            for (int i = 0; i < ins; i++) {

                if (i % 1000 == 0) {
                    print("DELETING " + i + ": " + cmp.printRecord(records[i], 0) + "\n");                                    
                }

                try {
                    btree.delete(records[i], leafFrame, interiorFrame, metaFrame);
                    delDone++;
                } catch (Exception e) {
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

        ISlotManagerFactory leafSlotManagerFactory = new OrderedSlotManagerFactory();
        ISlotManagerFactory interiorSlotManagerFactory = new OrderedSlotManagerFactory();
        IBTreeFrameLeafFactory leafFrameFactory = new BTreeNSMLeafFactory();
        IBTreeFrameInteriorFactory interiorFrameFactory = new BTreeNSMInteriorFactory();
        IBTreeFrameMetaFactory metaFrameFactory = new BTreeMetaFactory();
        
        IBTreeFrameLeaf leafFrame = leafFrameFactory.getFrame();
        IBTreeFrameInterior interiorFrame = interiorFrameFactory.getFrame();
        IBTreeFrameMeta metaFrame = metaFrameFactory.getFrame();          

        int keyLen = 2;

        IFieldAccessor[] fields = new IFieldAccessor[3];
        fields[0] = new Int32Accessor();
        fields[1] = new Int32Accessor();
        fields[2] = new Int32Accessor();

        IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        
        MultiComparator cmp = new MultiComparator(cmps, fields);

        BTree btree = new BTree(bufferCache, 
        		interiorFrameFactory, 
        		leafFrameFactory, 
        		interiorSlotManagerFactory,
                leafSlotManagerFactory, 
                cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        // generate sorted records
        int ins = 100000;
        byte[][] records = new byte[ins][];
        for (int i = 0; i < ins; i++) {
        	ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        	DataOutputStream dos = new DataOutputStream(baaos);        	        	
        	
        	int f0 = i;
        	int f1 = i;
        	int f2 = 5;
        	
        	IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
        	
            records[i] = baaos.toByteArray();
        }

        print("BULK LOADING " + ins + " RECORDS\n");
        long start = System.currentTimeMillis();
        
        BTree.BulkLoadContext ctx = btree.beginBulkLoad(0.7f, leafFrame, interiorFrame, metaFrame);                
        for (int i = 0; i < ins; i++) {
            btree.bulkLoadAddRecord(ctx, records[i]);
        }
        btree.endBulkLoad(ctx);
        
        long end = System.currentTimeMillis();
        long duration = end - start;
        print("DURATION: " + duration + "\n");
        
        // range search
        print("RANGE SEARCH:\n");
        IBTreeCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);
        
        ByteArrayAccessibleOutputStream lkbaaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream lkdos = new DataOutputStream(lkbaaos);    	    	    	
    	IntegerSerializerDeserializer.INSTANCE.serialize(44444, lkdos);
    	
    	ByteArrayAccessibleOutputStream hkbaaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream hkdos = new DataOutputStream(hkbaaos);    	    	    	
    	IntegerSerializerDeserializer.INSTANCE.serialize(44500, hkdos);
    	    	        
        byte[] lowKey = lkbaaos.toByteArray();
        byte[] highKey = hkbaaos.toByteArray();
                
        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps, fields);
        
        RangePredicate rangePred = new RangePredicate(false, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                byte[] array = rangeCursor.getPage().getBuffer().array();
                int recOffset = rangeCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
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
        
        ISlotManagerFactory leafSlotManagerFactory = new OrderedSlotManagerFactory();
        ISlotManagerFactory interiorSlotManagerFactory = new OrderedSlotManagerFactory();
        IBTreeFrameLeafFactory leafFrameFactory = new BTreeNSMLeafFactory();
        IBTreeFrameInteriorFactory interiorFrameFactory = new BTreeNSMInteriorFactory();
        IBTreeFrameMetaFactory metaFrameFactory = new BTreeMetaFactory();
        
        IBTreeFrameLeaf leafFrame = leafFrameFactory.getFrame();
        IBTreeFrameInterior interiorFrame = interiorFrameFactory.getFrame();
        IBTreeFrameMeta metaFrame = metaFrameFactory.getFrame();

        int keyLen = 2;
        
        IFieldAccessor[] fields = new IFieldAccessor[3];
        fields[0] = new Int32Accessor();
        fields[1] = new Int32Accessor();
        fields[2] = new Int32Accessor();

        IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();        
        MultiComparator cmp = new MultiComparator(cmps, fields);

        BTree btree = new BTree(bufferCache, 
        		interiorFrameFactory, 
        		leafFrameFactory, 
        		interiorSlotManagerFactory,
                leafSlotManagerFactory, 
                cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);
        
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
        	ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        	DataOutputStream dos = new DataOutputStream(baaos);        	        	
        	
        	int f0 = intervals[i][0];
        	int f1 = intervals[i][1];
        	int f2 = rnd.nextInt() % 100;
        	
        	IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
        	IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
        	IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
        	
        	byte[] record = baaos.toByteArray();
        	
            print("INSERTING " + i + " : " + f0 + " " + f1 + "\n");

            try {
                btree.insert(record, leafFrame, interiorFrame, metaFrame);
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
        IBTreeCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrame, interiorFrame);

        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                byte[] array = scanCursor.getPage().getBuffer().array();
                int recOffset = scanCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // try a range search
        print("RANGE SEARCH:\n");
        IBTreeCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);

        ByteArrayAccessibleOutputStream lkbaaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream lkdos = new DataOutputStream(lkbaaos);    	    	    	
    	IntegerSerializerDeserializer.INSTANCE.serialize(12, lkdos);
    	IntegerSerializerDeserializer.INSTANCE.serialize(12, lkdos);
    	
    	ByteArrayAccessibleOutputStream hkbaaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream hkdos = new DataOutputStream(hkbaaos);    	    	    	
    	IntegerSerializerDeserializer.INSTANCE.serialize(19, hkdos);
    	IntegerSerializerDeserializer.INSTANCE.serialize(19, hkdos);    	        
    	
        byte[] lowKey = lkbaaos.toByteArray();
        byte[] highKey = hkbaaos.toByteArray();
        
        IBinaryComparator[] searchCmps = new IBinaryComparator[2];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        searchCmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps, fields);
        
        print("INDEX RANGE SEARCH ON: " + cmp.printKey(lowKey, 0) + " " + cmp.printKey(highKey, 0) + "\n");                
        
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);
        btree.search(rangeCursor, rangePred, leafFrame, interiorFrame);
        
        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                byte[] array = rangeCursor.getPage().getBuffer().array();
                int recOffset = rangeCursor.getOffset();                
                String rec = cmp.printRecord(array, recOffset);
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