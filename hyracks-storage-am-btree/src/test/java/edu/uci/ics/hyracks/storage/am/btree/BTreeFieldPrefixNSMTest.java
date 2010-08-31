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
import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.api.IPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.types.Int32Accessor;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public class BTreeFieldPrefixNSMTest {
	
    //private static final int PAGE_SIZE = 8192;
    //private static final int PAGE_SIZE = 8192;
	//private static final int PAGE_SIZE = 32768; // 32K
	//private static final int PAGE_SIZE = 65536; // 64K
	private static final int PAGE_SIZE = 131072; // 128K
    private static final int NUM_PAGES = 40;
    
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
    
    private void tupleInsert(FieldPrefixNSMLeafFrame frame, MultiComparator cmp, int f0, int f1, int f2, boolean print, ArrayList<byte[]> records) throws Exception {
    	if(print) System.out.println("INSERTING: " + f0 + " " + f1 + " " + f2);
    	
    	ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
    	DataOutputStream dos = new DataOutputStream(baaos);        	        	
    	    	
    	IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
    	IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
    	IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
    	
    	byte[] record = baaos.toByteArray();        
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
        IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[2] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator cmp = new MultiComparator(cmps, fields);
        
        Random rnd = new Random();
        rnd.setSeed(50);
        
        ICachedPage page = bufferCache.pin(FileInfo.getDiskPageId(fileId, 0), false);
        try {
        	        	        	        	
            IPrefixSlotManager slotManager = new FieldPrefixSlotManager();
            FieldPrefixNSMLeafFrame frame = new FieldPrefixNSMLeafFrame();                                    
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
