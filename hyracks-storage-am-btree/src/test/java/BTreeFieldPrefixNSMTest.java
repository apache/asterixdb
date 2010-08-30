package edu.uci.ics.asterix.test.storage;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.indexing.btree.frames.FieldPrefixNSMLeaf;
import edu.uci.ics.asterix.indexing.btree.impls.FieldPrefixSlotManager;
import edu.uci.ics.asterix.indexing.btree.impls.MultiComparator;
import edu.uci.ics.asterix.indexing.btree.interfaces.IComparator;
import edu.uci.ics.asterix.indexing.btree.interfaces.IFieldAccessor;
import edu.uci.ics.asterix.indexing.btree.interfaces.IPrefixSlotManager;
import edu.uci.ics.asterix.indexing.types.Int32Accessor;
import edu.uci.ics.asterix.indexing.types.Int32Comparator;
import edu.uci.ics.asterix.om.AInt32;
import edu.uci.ics.asterix.storage.buffercache.BufferAllocator;
import edu.uci.ics.asterix.storage.buffercache.BufferCache;
import edu.uci.ics.asterix.storage.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.asterix.storage.buffercache.IBufferCache;
import edu.uci.ics.asterix.storage.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.asterix.storage.buffercache.ICachedPage;
import edu.uci.ics.asterix.storage.buffercache.IPageReplacementStrategy;
import edu.uci.ics.asterix.storage.file.FileInfo;
import edu.uci.ics.asterix.storage.file.FileManager;

public class BTreeFieldPrefixNSMTest {
	
    //private static final int PAGE_SIZE = 8192;
    //private static final int PAGE_SIZE = 8192;
	//private static final int PAGE_SIZE = 32768; // 32K
	//private static final int PAGE_SIZE = 65536; // 64K
	private static final int PAGE_SIZE = 131072; // 128K
    private static final int NUM_PAGES = 40;
    
    // to help with the logger madness
    private void print(String str) {
    	if(GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.FINEST)) {            
        	GlobalConfig.ASTERIX_LOGGER.finest(str);
        }
    }       
    
    private void tupleInsert(FieldPrefixNSMLeaf frame, MultiComparator cmp, int f0, int f1, int f2, boolean print, ArrayList<byte[]> records) throws Exception {
    	if(print) System.out.println("INSERTING: " + f0 + " " + f1 + " " + f2);
    	
    	byte[] record = new byte[12];
        AInt32 field0 = new AInt32(f0);
        AInt32 field1 = new AInt32(f1);
        AInt32 field2 = new AInt32(f2);
        System.arraycopy(field0.toBytes(), 0, record, 0, 4);
        System.arraycopy(field1.toBytes(), 0, record, 4, 4);
        System.arraycopy(field2.toBytes(), 0, record, 8, 4);
        frame.insert(record, cmp);
        
        if(records != null) records.add(record);
    }
    
    @Test
    public void test01() throws Exception {
    	
        FileManager fileManager = new FileManager();
        ICacheMemoryAllocator allocator = new BufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        IBufferCache bufferCache = new BufferCache(allocator, prs, fileManager, PAGE_SIZE, NUM_PAGES);
        
        File f = new File("/tmp/btreetest.bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int fileId = 0;
        FileInfo fi = new FileInfo(fileId, raf);
        fileManager.registerFile(fi);
                
        int numFields = 3;
        IFieldAccessor[] fields = new IFieldAccessor[numFields];
        fields[0] = new Int32Accessor(); // first key field
        fields[1] = new Int32Accessor(); // second key field
        fields[2] = new Int32Accessor(); // third key field
        
        int keyLen = 3;
        IComparator[] cmps = new IComparator[keyLen];
        cmps[0] = new Int32Comparator();
        cmps[1] = new Int32Comparator();
        cmps[2] = new Int32Comparator();
        MultiComparator cmp = new MultiComparator(cmps, fields);
        
        Random rnd = new Random();
        rnd.setSeed(50);
        
        ICachedPage page = bufferCache.pin(FileInfo.getDiskPageId(fileId, 0), false);
        try {
        	        	        	        	
            IPrefixSlotManager slotManager = new FieldPrefixSlotManager();
            FieldPrefixNSMLeaf frame = new FieldPrefixNSMLeaf();                                    
            frame.setPage(page);            
            frame.initBuffer((byte)0);
            slotManager.setFrame(frame);          
            frame.setNumPrefixRecords(0);
            
            String before = new String();
            String after = new String();
            
            int compactFreq = 5;
        	int compressFreq = 5;
        	int smallMax = 10;        	
        	int numRecords = 5000;
        	ArrayList<byte[]> records = new ArrayList<byte[]>();
        	records.ensureCapacity(numRecords);
        	        	
        	// insert records with random calls to compact and compress
        	for(int i = 0; i < numRecords; i++) {
        		
        		if((i+1) % 100 == 0) print("INSERTING " + (i+1) + " / " + numRecords + "\n");        		
        		
        		int a = rnd.nextInt() % smallMax;
        		int b = rnd.nextInt() % smallMax;
        		int c = i;
        		tupleInsert(frame, cmp, a, b, c, false, records);
        		
        		if(rnd.nextInt() % compactFreq == 0) {
        			before = frame.printKeys(cmp);
        			frame.compact(cmp);
        			after = frame.printKeys(cmp);
        			Assert.assertEquals(before, after);
        		}
        		
        		if(rnd.nextInt() % compressFreq == 0) {
        			before = frame.printKeys(cmp);
        			frame.compress(cmp);
        			after = frame.printKeys(cmp);
        			Assert.assertEquals(before, after);
        		}
        	}
        	
        	// delete records with random calls to compact and compress        	
        	for(int i = 0; i < records.size(); i++) {        	            	    
        		
        		if((i+1) % 100 == 0) print("DELETING " + (i+1) + " / " + numRecords + "\n");
        		
        		frame.delete(records.get(i), cmp, true);    
        		
        		if(rnd.nextInt() % compactFreq == 0) {
        			before = frame.printKeys(cmp);
        			frame.compact(cmp);
        			after = frame.printKeys(cmp);
        			Assert.assertEquals(before, after);
        		}
        		
        		if(rnd.nextInt() % compressFreq == 0) {
        			before = frame.printKeys(cmp);
        			frame.compress(cmp);
        			after = frame.printKeys(cmp);
        			Assert.assertEquals(before, after);
        		}  
        	}
        	        	
        } finally {            
            bufferCache.unpin(page);
        }              
        
        bufferCache.close();
        fileManager.close();
    }
}
