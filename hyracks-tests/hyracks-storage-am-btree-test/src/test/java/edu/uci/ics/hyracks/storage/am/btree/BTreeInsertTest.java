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

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.util.AbstractBTreeTest;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestContext;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestUtils;

/**
 * Tests the BTree insert operation in the following scenarios:
 * 1.   Integer fields:
 * 1.1. One key, one value
 * 1.2. Two keys, no value
 * 1.3. Two keys, two values
 * 2.   String fields:
 * 2.1. One key, one value
 * 2.2. Two keys, no value
 * 2.3. Two keys, two values
 * 
 * Each tests consists of the following steps:
 * 1. Insert randomly generated tuples.
 * 2. Perform ordered scan and print results.
 * 3. Perform disk-order scan and print results.
 * 4. Perform range search and print results.
 * 
 */
@SuppressWarnings("rawtypes")
public class BTreeInsertTest extends AbstractBTreeTest {        
    
    @Test
    public void oneIntKeyAndValue() throws Exception {        
        LOGGER.info("BTree Test With One Int Key And Value.");
        
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        BTreeTestContext testCtx = BTreeTestUtils.createBTreeTestContext(bufferCache, btreeFileId, fieldSerdes, 1);
        BTreeTestUtils.fillIntBTree(testCtx, 10000);

        BTreeTestUtils.checkOrderedScan(testCtx);
        BTreeTestUtils.checkDiskOrderScan(testCtx);
        
        // Range search in [-1000, 1000];
        ITupleReference lowKey = TupleUtils.createIntegerTuple(-1000);
        ITupleReference highKey = TupleUtils.createIntegerTuple(1000);
        BTreeTestUtils.checkRangeSearch(testCtx, lowKey, highKey, true, true);        
        
        testCtx.btree.close();
    }
    
    @Test
    public void twoIntKeys() throws Exception {        
        LOGGER.info("BTree Test With Two Int Keys.");
        
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        BTreeTestContext testCtx = BTreeTestUtils.createBTreeTestContext(bufferCache, btreeFileId, fieldSerdes, 2);
        BTreeTestUtils.fillIntBTree(testCtx, 10000);
        
        BTreeTestUtils.checkOrderedScan(testCtx);
        BTreeTestUtils.checkDiskOrderScan(testCtx);

        // Range search in [50 0, 50 500];
        ITupleReference lowKey = TupleUtils.createIntegerTuple(50, 0);
        ITupleReference highKey = TupleUtils.createIntegerTuple(50, 500);
        BTreeTestUtils.checkRangeSearch(testCtx, lowKey, highKey, true, true);        
        
        // Prefix range search in [50, 50]
        ITupleReference prefixLowKey = TupleUtils.createIntegerTuple(50);
        ITupleReference prefixHighKey = TupleUtils.createIntegerTuple(50);
        BTreeTestUtils.checkRangeSearch(testCtx, prefixLowKey, prefixHighKey, true, true);
        
        testCtx.btree.close();
    }
    
    @Test
    public void twoIntKeysAndValues() throws Exception {        
        LOGGER.info("BTree Test With Two Int Keys And Values.");
        
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        BTreeTestContext testCtx = BTreeTestUtils.createBTreeTestContext(bufferCache, btreeFileId, fieldSerdes, 2);
        BTreeTestUtils.fillIntBTree(testCtx, 10000);
        
        BTreeTestUtils.checkOrderedScan(testCtx);
        BTreeTestUtils.checkDiskOrderScan(testCtx);

        // Range search in [50 100, 100 100];
        ITupleReference lowKey = TupleUtils.createIntegerTuple(-100, -100);
        ITupleReference highKey = TupleUtils.createIntegerTuple(100, 100);
        BTreeTestUtils.checkRangeSearch(testCtx, lowKey, highKey, true, true);        
        
        // Prefix range search in [50, 50]
        ITupleReference prefixLowKey = TupleUtils.createIntegerTuple(50);
        ITupleReference prefixHighKey = TupleUtils.createIntegerTuple(50);
        BTreeTestUtils.checkRangeSearch(testCtx, prefixLowKey, prefixHighKey, true, true);
        
        testCtx.btree.close();
    }
    
    @Test
    public void oneStringKeyAndValue() throws Exception {        
        LOGGER.info("BTree Test With One String Key And Value.");
        
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        BTreeTestContext testCtx = BTreeTestUtils.createBTreeTestContext(bufferCache, btreeFileId, fieldSerdes, 1);
        BTreeTestUtils.fillStringBTree(testCtx, 10000);
        
        BTreeTestUtils.checkOrderedScan(testCtx);
        BTreeTestUtils.checkDiskOrderScan(testCtx);

        // Range search in ["cbf", cc7"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7");
        BTreeTestUtils.checkRangeSearch(testCtx, lowKey, highKey, true, true);        
        
        testCtx.btree.close();
    }
    
    @Test
    public void twoStringKeys() throws Exception {        
        LOGGER.info("BTree Test With Two String Keys.");
        
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        BTreeTestContext testCtx = BTreeTestUtils.createBTreeTestContext(bufferCache, btreeFileId, fieldSerdes, 2);
        BTreeTestUtils.fillStringBTree(testCtx, 10000);
        
        BTreeTestUtils.checkOrderedScan(testCtx);
        BTreeTestUtils.checkDiskOrderScan(testCtx);

        // Range search in ["cbf", "ddd", cc7", "eee"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf", "ddd");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7", "eee");
        BTreeTestUtils.checkRangeSearch(testCtx, lowKey, highKey, true, true);   
        
        // Prefix range search in ["cbf", cc7"]
        ITupleReference prefixLowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference prefixHighKey = TupleUtils.createTuple(fieldSerdes, "cc7");
        BTreeTestUtils.checkRangeSearch(testCtx, prefixLowKey, prefixHighKey, true, true); 
        
        testCtx.btree.close();
    }
    
    @Test
    public void twoStringKeysAndValues() throws Exception {        
        LOGGER.info("BTree Test With Two String Keys And Values.");
        
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        BTreeTestContext testCtx = BTreeTestUtils.createBTreeTestContext(bufferCache, btreeFileId, fieldSerdes, 2);
        BTreeTestUtils.fillStringBTree(testCtx, 10000);
        
        BTreeTestUtils.checkOrderedScan(testCtx);
        BTreeTestUtils.checkDiskOrderScan(testCtx);

        // Range search in ["cbf", "ddd", cc7", "eee"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf", "ddd");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7", "eee");
        BTreeTestUtils.checkRangeSearch(testCtx, lowKey, highKey, true, true);        
        
        // Prefix range search in ["cbf", cc7"]
        ITupleReference prefixLowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference prefixHighKey = TupleUtils.createTuple(fieldSerdes, "cc7");
        BTreeTestUtils.checkRangeSearch(testCtx, prefixLowKey, prefixHighKey, true, true); 
        
        testCtx.btree.close();
    }
}
