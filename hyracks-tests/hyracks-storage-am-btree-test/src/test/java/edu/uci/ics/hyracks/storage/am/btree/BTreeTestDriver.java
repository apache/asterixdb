package edu.uci.ics.hyracks.storage.am.btree;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.util.AbstractBTreeTest;

@SuppressWarnings("rawtypes")
public abstract class BTreeTestDriver extends AbstractBTreeTest {

    protected static final int numTuplesToInsert = 10000;
    
    protected abstract void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType, ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey, ITupleReference prefixHighKey) throws Exception;
    protected abstract String getTestOpName();
    
    @Test
    public void oneIntKeyAndValue() throws Exception {        
        LOGGER.info("BTree " + getTestOpName() + " Test With One Int Key And Value.");
                
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        // Range search in [-1000, 1000]
        ITupleReference lowKey = TupleUtils.createIntegerTuple(-1000);
        ITupleReference highKey = TupleUtils.createIntegerTuple(1000);
        
        runTest(fieldSerdes, 1, BTreeLeafFrameType.REGULAR_NSM, lowKey, highKey, null, null);
        runTest(fieldSerdes, 1, BTreeLeafFrameType.FIELD_PREFIX_COMPRESSED_NSM, lowKey, highKey, null, null);
    }
    
    @Test
    public void twoIntKeys() throws Exception {        
        LOGGER.info("BTree " + getTestOpName() + " Test With Two Int Keys.");
        
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        
        // Range search in [50 0, 50 500]
        ITupleReference lowKey = TupleUtils.createIntegerTuple(50, 0);
        ITupleReference highKey = TupleUtils.createIntegerTuple(50, 500);
        
        // Prefix range search in [50, 50]
        ITupleReference prefixLowKey = TupleUtils.createIntegerTuple(50);
        ITupleReference prefixHighKey = TupleUtils.createIntegerTuple(50);
        
        runTest(fieldSerdes, 2, BTreeLeafFrameType.REGULAR_NSM, lowKey, highKey, prefixLowKey, prefixHighKey);
        runTest(fieldSerdes, 2, BTreeLeafFrameType.FIELD_PREFIX_COMPRESSED_NSM, lowKey, highKey, prefixLowKey, prefixHighKey);
    }
    
    @Test
    public void twoIntKeysAndValues() throws Exception {        
        LOGGER.info("BTree " + getTestOpName() + " Test With Two Int Keys And Values.");
        
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        
        // Range search in [50 100, 100 100]
        ITupleReference lowKey = TupleUtils.createIntegerTuple(-100, -100);
        ITupleReference highKey = TupleUtils.createIntegerTuple(100, 100);
        
        // Prefix range search in [50, 50]
        ITupleReference prefixLowKey = TupleUtils.createIntegerTuple(50);
        ITupleReference prefixHighKey = TupleUtils.createIntegerTuple(50);
        
        runTest(fieldSerdes, 2, BTreeLeafFrameType.REGULAR_NSM, lowKey, highKey, prefixLowKey, prefixHighKey);
        runTest(fieldSerdes, 2, BTreeLeafFrameType.FIELD_PREFIX_COMPRESSED_NSM, lowKey, highKey, prefixLowKey, prefixHighKey);
    }        
    
    @Test
    public void oneStringKeyAndValue() throws Exception {        
        LOGGER.info("BTree " + getTestOpName() + " Test With One String Key And Value.");
        
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        
        // Range search in ["cbf", cc7"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7");
        
        runTest(fieldSerdes, 1, BTreeLeafFrameType.REGULAR_NSM, lowKey, highKey, null, null);
        runTest(fieldSerdes, 1, BTreeLeafFrameType.FIELD_PREFIX_COMPRESSED_NSM, lowKey, highKey, null, null);
    }
    
    @Test
    public void twoStringKeys() throws Exception {        
        LOGGER.info("BTree " + getTestOpName() + " Test With Two String Keys.");
        
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        
        // Range search in ["cbf", "ddd", cc7", "eee"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf", "ddd");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7", "eee");
        
        // Prefix range search in ["cbf", cc7"]
        ITupleReference prefixLowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference prefixHighKey = TupleUtils.createTuple(fieldSerdes, "cc7");
        
        runTest(fieldSerdes, 2, BTreeLeafFrameType.REGULAR_NSM, lowKey, highKey, prefixLowKey, prefixHighKey);
        runTest(fieldSerdes, 2, BTreeLeafFrameType.FIELD_PREFIX_COMPRESSED_NSM, lowKey, highKey, prefixLowKey, prefixHighKey);
    }
    
    @Test
    public void twoStringKeysAndValues() throws Exception {        
        LOGGER.info("BTree " + getTestOpName() + " Test With Two String Keys And Values.");
        
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        
        // Range search in ["cbf", "ddd", cc7", "eee"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf", "ddd");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7", "eee");
        
        // Prefix range search in ["cbf", cc7"]
        ITupleReference prefixLowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference prefixHighKey = TupleUtils.createTuple(fieldSerdes, "cc7");
        
        runTest(fieldSerdes, 2, BTreeLeafFrameType.REGULAR_NSM, lowKey, highKey, prefixLowKey, prefixHighKey);
        runTest(fieldSerdes, 2, BTreeLeafFrameType.FIELD_PREFIX_COMPRESSED_NSM, lowKey, highKey, prefixLowKey, prefixHighKey);
    }
}
