package edu.uci.ics.asterix.test.storage;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.logging.Level;

import org.junit.Test;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.indexing.btree.impls.BTree;
import edu.uci.ics.asterix.indexing.btree.impls.BTreeDiskOrderScanCursor;
import edu.uci.ics.asterix.indexing.btree.impls.BTreeMetaFactory;
import edu.uci.ics.asterix.indexing.btree.impls.BTreeNSMInteriorFactory;
import edu.uci.ics.asterix.indexing.btree.impls.BTreeNSMLeafFactory;
import edu.uci.ics.asterix.indexing.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.asterix.indexing.btree.impls.MultiComparator;
import edu.uci.ics.asterix.indexing.btree.impls.OrderedSlotManagerFactory;
import edu.uci.ics.asterix.indexing.btree.impls.RangePredicate;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeCursor;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameInterior;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameInteriorFactory;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameLeafFactory;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameMeta;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameMetaFactory;
import edu.uci.ics.asterix.indexing.btree.interfaces.IComparator;
import edu.uci.ics.asterix.indexing.btree.interfaces.IFieldAccessor;
import edu.uci.ics.asterix.indexing.btree.interfaces.ISlotManagerFactory;
import edu.uci.ics.asterix.indexing.types.Int32Accessor;
import edu.uci.ics.asterix.indexing.types.Int32Comparator;
import edu.uci.ics.asterix.indexing.types.StringAccessor;
import edu.uci.ics.asterix.indexing.types.StringComparator;
import edu.uci.ics.asterix.om.AInt32;
import edu.uci.ics.asterix.storage.buffercache.BufferAllocator;
import edu.uci.ics.asterix.storage.buffercache.BufferCache;
import edu.uci.ics.asterix.storage.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.asterix.storage.buffercache.IBufferCache;
import edu.uci.ics.asterix.storage.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.asterix.storage.buffercache.IPageReplacementStrategy;
import edu.uci.ics.asterix.storage.file.FileInfo;
import edu.uci.ics.asterix.storage.file.FileManager;

public class BTreeTest {    
    private static final int PAGE_SIZE = 128;
    // private static final int PAGE_SIZE = 8192;
    private static final int NUM_PAGES = 10;
    
    // to help with the logger madness
    private void print(String str) {
    	if(GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.FINEST)) {
            GlobalConfig.ASTERIX_LOGGER.finest(str);
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
        IComparator[] cmps = new IComparator[keyLen];
        cmps[0] = new Int32Comparator();
        MultiComparator cmp = new MultiComparator(cmps, fields);

        BTree btree = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, interiorSlotManagerFactory, leafSlotManagerFactory, cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();                
        
        print("INSERTING INTO TREE\n");

        byte[] record = new byte[8];
        for (int i = 0; i < 10000; i++) {
            AInt32 field0 = new AInt32(rnd.nextInt() % 10000);
            AInt32 field1 = new AInt32(5);

            byte[] f0 = field0.toBytes();
            byte[] f1 = field1.toBytes();
            
            System.arraycopy(f0, 0, record, 0, 4);
            System.arraycopy(f1, 0, record, 4, 4);
            
            if (i % 1000 == 0) {
                long end = System.currentTimeMillis();
                print("INSERTING " + i + " : " + field0.getIntegerValue() + " " + field1.getIntegerValue() + " " + (end - start) + "\n");            	
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

        AInt32 lk = new AInt32(-1000);
        byte[] lowKey = lk.toBytes();

        AInt32 hk = new AInt32(1000);
        byte[] highKey = hk.toBytes();

        IComparator[] searchCmps = new IComparator[1];
        searchCmps[0] = new Int32Comparator();
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
        IComparator[] cmps = new IComparator[keyLen];
        cmps[0] = new Int32Comparator();
        cmps[1] = new Int32Comparator();        
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
        byte[] record = new byte[12];
        for (int i = 0; i < 10000; i++) {
            AInt32 field0 = new AInt32(rnd.nextInt() % 2000);
            AInt32 field1 = new AInt32(rnd.nextInt() % 1000);
            AInt32 field2 = new AInt32(5);

            byte[] f0 = field0.toBytes();
            byte[] f1 = field1.toBytes();
            byte[] f2 = field2.toBytes();
            
            System.arraycopy(f0, 0, record, 0, 4);
            System.arraycopy(f1, 0, record, 4, 4);
            System.arraycopy(f2, 0, record, 8, 4);             
            
            if (i % 1000 == 0) {
            	print("INSERTING " + i + " : " + field0.getIntegerValue() + " " + field1.getIntegerValue() + "\n");
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

        AInt32 lk = new AInt32(-3);
        byte[] lowKey = lk.toBytes();
        
        AInt32 hk = new AInt32(3);
        byte[] highKey = hk.toBytes();
                
        IComparator[] searchCmps = new IComparator[1];
        searchCmps[0] = new Int32Comparator();
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
    	IComparator[] cmps = new IComparator[keyLen];
    	cmps[0] = new StringComparator();

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
    		String field0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
    		String field1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
    		
    		byte[] f0 = field0.getBytes();
    		byte[] f1 = field1.getBytes();

    		byte[] record = new byte[f0.length + f1.length + 8];
    		ByteBuffer buf = ByteBuffer.wrap(record);

    		int start = 0;
    		buf.putInt(start, f0.length);
    		start += 4;
    		System.arraycopy(f0, 0, record, start, f0.length);
    		start += f0.length;
    		buf.putInt(start, f1.length);
    		start += 4;
    		System.arraycopy(f1, 0, record, start, f1.length);
    		start += f1.length;

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
                
        byte[] lowKey = new byte[7];        
        ByteBuffer lkByteBuf = ByteBuffer.wrap(lowKey);
        lkByteBuf.putInt(0, 3);
        System.arraycopy("cbf".getBytes(), 0, lowKey, 4, 3);
        
        byte[] highKey = new byte[7];
        ByteBuffer hkByteBuf = ByteBuffer.wrap(highKey);
        hkByteBuf.putInt(0, 3);
        System.arraycopy("cc7".getBytes(), 0, highKey, 4, 3);
                     
        IComparator[] searchCmps = new IComparator[1];
        searchCmps[0] = new StringComparator();
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
        IComparator[] cmps = new IComparator[keyLen];
        cmps[0] = new StringComparator();

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
                String field0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                String field1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);

                byte[] f0 = field0.getBytes();
                byte[] f1 = field1.getBytes();

                byte[] record = new byte[f0.length + f1.length + 8];
                ByteBuffer buf = ByteBuffer.wrap(record);

                int start = 0;
                buf.putInt(start, f0.length);
                start += 4;
                System.arraycopy(f0, 0, record, start, f0.length);
                start += f0.length;
                buf.putInt(start, f1.length);
                start += 4;
                System.arraycopy(f1, 0, record, start, f1.length);
                start += f1.length;
                
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

        IComparator[] cmps = new IComparator[keyLen];
        cmps[0] = new Int32Comparator();
        cmps[1] = new Int32Comparator();

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
            byte[] record = new byte[12];

            AInt32 field0 = new AInt32(i);
            AInt32 field1 = new AInt32(i);
            AInt32 field2 = new AInt32(rnd.nextInt() % 100);

            byte[] f0 = field0.toBytes();
            byte[] f1 = field1.toBytes();
            byte[] f2 = field2.toBytes();

            System.arraycopy(f0, 0, record, 0, 4);
            System.arraycopy(f1, 0, record, 4, 4);
            System.arraycopy(f2, 0, record, 8, 4);
            records[i] = record;
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
        
        AInt32 lowKey = new AInt32(44444);
        AInt32 highKey = new AInt32(44500);

        IComparator[] searchCmps = new IComparator[1];
        searchCmps[0] = new Int32Comparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps, fields);

        RangePredicate rangePred = new RangePredicate(false, lowKey.toBytes(), highKey.toBytes(), searchCmp);
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

        IComparator[] cmps = new IComparator[keyLen];
        cmps[0] = new Int32Comparator();
        cmps[1] = new Int32Comparator();

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

        byte[] record = new byte[12];

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
            AInt32 field0 = new AInt32(intervals[i][0]);
            AInt32 field1 = new AInt32(intervals[i][1]);
            AInt32 field2 = new AInt32(rnd.nextInt() % 100);

            byte[] f0 = field0.toBytes();
            byte[] f1 = field1.toBytes();
            byte[] f2 = field2.toBytes();

            System.arraycopy(f0, 0, record, 0, 4);
            System.arraycopy(f1, 0, record, 4, 4);
            System.arraycopy(f2, 0, record, 8, 4);

            print("INSERTING " + i + " : " + field0.getIntegerValue() + " " + field1.getIntegerValue() + "\n");

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

        AInt32 lowerBound = new AInt32(12);
        AInt32 upperBound = new AInt32(19);
        byte[] lbBytes = lowerBound.toBytes();
        byte[] hbBytes = upperBound.toBytes();

        byte[] lowKey = new byte[8];
        byte[] highKey = new byte[8];

        System.arraycopy(lbBytes, 0, lowKey, 0, 4);
        System.arraycopy(lbBytes, 0, lowKey, 4, 4);

        System.arraycopy(hbBytes, 0, highKey, 0, 4);
        System.arraycopy(hbBytes, 0, highKey, 4, 4);

        IComparator[] searchCmps = new IComparator[2];
        searchCmps[0] = new Int32Comparator();
        searchCmps[1] = new Int32Comparator();
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