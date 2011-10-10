package edu.uci.ics.hyracks.storage.am.btree.util;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public class BTreeTestUtils {
    private static final Logger LOGGER = Logger.getLogger(BTreeTestUtils.class.getName());    
    
    public static BTreeTestContext createBTreeTestContext(IBufferCache bufferCache, int btreeFileId, ISerializerDeserializer[] fieldSerdes, int numKeyFields, BTreeLeafFrameType leafType) throws Exception {        
        ITypeTrait[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes, fieldSerdes.length);
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);
        
        BTree btree = BTreeUtils.createBTree(bufferCache, btreeFileId, typeTraits, cmpFactories, leafType);
        
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) btree.getInteriorFrameFactory().createFrame();
        ITreeIndexMetaDataFrame metaFrame = btree.getFreePageManager().getMetaDataFrameFactory().createFrame();

        btree.create(btreeFileId, leafFrame, metaFrame);
        btree.open(btreeFileId);
        
        BTreeTestContext testCtx = new BTreeTestContext(bufferCache, fieldSerdes, btree, leafFrame, interiorFrame, metaFrame);
        return testCtx;
    }
    
    private static void compareActualAndExpected(ITupleReference actual, CheckTuple expected, ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
        for (int i = 0; i < fieldSerdes.length; i++) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(
                    actual.getFieldData(i), actual.getFieldStart(i),
                    actual.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Object actualObj = fieldSerdes[i].deserialize(dataIn);            
            if (!actualObj.equals(expected.get(i))) {
                fail("Actual and expected fields do not match.\nExpected: " + expected.get(i) + "\nActual  : " + actualObj);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private static CheckTuple createCheckTupleFromTuple(ITupleReference tuple, ISerializerDeserializer[] fieldSerdes, int numKeys) throws HyracksDataException {
        CheckTuple checkTuple = new CheckTuple(fieldSerdes.length, numKeys);
        int numFields = Math.min(fieldSerdes.length, tuple.getFieldCount());
        for (int i = 0; i < numFields; i++) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(
                    tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Comparable fieldObj = (Comparable)fieldSerdes[i].deserialize(dataIn);
            checkTuple.add(fieldObj);
        }
        return checkTuple;
    }
    
    @SuppressWarnings("unchecked")
    private static void createTupleFromCheckTuple(CheckTuple checkTuple, ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple, ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
        int fieldCount = tupleBuilder.getFieldEndOffsets().length; 
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (int i = 0; i < fieldCount; i++) {
            fieldSerdes[i].serialize(checkTuple.get(i), dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }
    
    public static void checkOrderedScan(BTreeTestContext testCtx) throws Exception {
        LOGGER.info("Testing Ordered Scan.");
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(testCtx.leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        BTreeOpContext searchOpCtx = testCtx.btree.createOpContext(IndexOp.SEARCH, testCtx.leafFrame, testCtx.interiorFrame, null);
        testCtx.btree.search(scanCursor, nullPred, searchOpCtx);
        Iterator<CheckTuple> checkIter = testCtx.checkTuples.iterator();
        int actualCount = 0;
        try {
            while (scanCursor.hasNext()) {
                if (!checkIter.hasNext()) {
                    fail("Ordered scan returned more answers than expected.\nExpected: " + testCtx.checkTuples.size());
                }
                scanCursor.next();
                CheckTuple expectedTuple = checkIter.next();
                ITupleReference tuple = scanCursor.getTuple();
                compareActualAndExpected(tuple, expectedTuple, testCtx.fieldSerdes);
                actualCount++;
            }
            if (actualCount < testCtx.checkTuples.size()) {
                fail("Ordered scan returned fewer answers than expected.\nExpected: " + testCtx.checkTuples.size() + "\nActual  : " + actualCount);
            }
        } finally {
            scanCursor.close();
        }
    }
    
    public static void checkDiskOrderScan(BTreeTestContext testCtx) throws Exception {
        LOGGER.info("Testing Disk-Order Scan.");
        ITreeIndexCursor diskOrderCursor = new TreeDiskOrderScanCursor(testCtx.leafFrame);
        BTreeOpContext diskOrderScanOpCtx = testCtx.btree.createOpContext(IndexOp.DISKORDERSCAN, testCtx.leafFrame, null, null);
        testCtx.btree.diskOrderScan(diskOrderCursor, testCtx.leafFrame, testCtx.metaFrame, diskOrderScanOpCtx);
        int actualCount = 0;        
        try {
            while (diskOrderCursor.hasNext()) {
                diskOrderCursor.next();
                ITupleReference tuple = diskOrderCursor.getTuple();
                CheckTuple checkTuple = createCheckTupleFromTuple(tuple, testCtx.fieldSerdes, testCtx.btree.getMultiComparator().getKeyFieldCount());
                if (!testCtx.checkTuples.contains(checkTuple)) {
                    fail("Disk-order scan returned unexpected answer: " + checkTuple.toString());
                }
                actualCount++;
            }
            if (actualCount < testCtx.checkTuples.size()) {
                fail("Disk-order scan returned fewer answers than expected.\nExpected: " + testCtx.checkTuples.size() + "\nActual  : " + actualCount);
            }
            if (actualCount > testCtx.checkTuples.size()) {
                fail("Disk-order scan returned more answers than expected.\nExpected: " + testCtx.checkTuples.size() + "\nActual  : " + actualCount);
            }
        } finally {
            diskOrderCursor.close();
        }
    }
    
    public static void checkRangeSearch(BTreeTestContext testCtx, ITupleReference lowKey, ITupleReference highKey, boolean lowKeyInclusive, boolean highKeyInclusive) throws Exception {
        LOGGER.info("Testing Range Search.");
        MultiComparator lowKeyCmp = BTreeUtils.getSearchMultiComparator(testCtx.btree.getMultiComparator(), lowKey);
        MultiComparator highKeyCmp = BTreeUtils.getSearchMultiComparator(testCtx.btree.getMultiComparator(), highKey);
        ITreeIndexCursor searchCursor = new BTreeRangeSearchCursor(testCtx.leafFrame);
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, lowKeyInclusive, highKeyInclusive, lowKeyCmp, highKeyCmp);
        BTreeOpContext searchOpCtx = testCtx.btree.createOpContext(IndexOp.SEARCH, testCtx.leafFrame, testCtx.interiorFrame, null);
        testCtx.btree.search(searchCursor, rangePred, searchOpCtx);
        // Get the subset of elements from the expected set within given key range.
        CheckTuple lowKeyCheck = createCheckTupleFromTuple(lowKey, testCtx.fieldSerdes, lowKeyCmp.getKeyFieldCount());
        CheckTuple highKeyCheck = createCheckTupleFromTuple(highKey, testCtx.fieldSerdes, highKeyCmp.getKeyFieldCount());
        NavigableSet<CheckTuple> expectedSubset = null;
        if (lowKeyCmp.getKeyFieldCount() < testCtx.btree.getMultiComparator().getKeyFieldCount() || 
                highKeyCmp.getKeyFieldCount() < testCtx.btree.getMultiComparator().getKeyFieldCount()) {
            // Searching on a key prefix (low key or high key or both).
            expectedSubset = getPrefixExpectedSubset(testCtx.checkTuples, lowKeyCheck, highKeyCheck);
        } else {
            // Searching on all key fields.
            expectedSubset = testCtx.checkTuples.subSet(lowKeyCheck, lowKeyInclusive, highKeyCheck, highKeyInclusive);
        }
        Iterator<CheckTuple> checkIter = expectedSubset.iterator();
        int actualCount = 0;
        try {
            while (searchCursor.hasNext()) {
                if (!checkIter.hasNext()) {
                    fail("Range search returned more answers than expected.\nExpected: " + expectedSubset.size());
                }
                searchCursor.next();
                CheckTuple expectedTuple = checkIter.next();
                ITupleReference tuple = searchCursor.getTuple();
                compareActualAndExpected(tuple, expectedTuple, testCtx.fieldSerdes);
                actualCount++;
            }
            if (actualCount < expectedSubset.size()) {
                fail("Range search returned fewer answers than expected.\nExpected: " + expectedSubset.size() + "\nActual  : " + actualCount);
            }
        } finally {
            searchCursor.close();
        }
    }
    
    public static void checkPointSearches(BTreeTestContext testCtx) throws Exception {
        LOGGER.info("Testing Point Searches On All Expected Keys.");        
        ITreeIndexCursor searchCursor = new BTreeRangeSearchCursor(testCtx.leafFrame);
        
        ArrayTupleBuilder lowKeyBuilder = new ArrayTupleBuilder(testCtx.btree.getMultiComparator().getKeyFieldCount());
        ArrayTupleReference lowKey = new ArrayTupleReference();
        ArrayTupleBuilder highKeyBuilder = new ArrayTupleBuilder(testCtx.btree.getMultiComparator().getKeyFieldCount());
        ArrayTupleReference highKey = new ArrayTupleReference();
        
        BTreeOpContext searchOpCtx = testCtx.btree.createOpContext(IndexOp.SEARCH, testCtx.leafFrame, testCtx.interiorFrame, null);
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, null, null);

        // Iterate through expected tuples, and perform a point search in the BTree to verify the tuple can be reached.
        for (CheckTuple checkTuple : testCtx.checkTuples) {
            createTupleFromCheckTuple(checkTuple, lowKeyBuilder, lowKey, testCtx.fieldSerdes);
            createTupleFromCheckTuple(checkTuple, highKeyBuilder, highKey, testCtx.fieldSerdes);
            MultiComparator lowKeyCmp = BTreeUtils.getSearchMultiComparator(testCtx.btree.getMultiComparator(), lowKey);
            MultiComparator highKeyCmp = BTreeUtils.getSearchMultiComparator(testCtx.btree.getMultiComparator(), highKey);
                        
            rangePred.setLowKey(lowKey, true);
            rangePred.setHighKey(highKey, true);
            rangePred.setLowKeyComparator(lowKeyCmp);
            rangePred.setHighKeyComparator(highKeyCmp);
            
            testCtx.btree.search(searchCursor, rangePred, searchOpCtx);
            
            try {
                // We expect exactly one answer.
                if (searchCursor.hasNext()) {
                    searchCursor.next();
                    ITupleReference tuple = searchCursor.getTuple();
                    compareActualAndExpected(tuple, checkTuple, testCtx.fieldSerdes);
                }
                if (searchCursor.hasNext()) {
                    fail("Point search returned more than one answer.");
                }
            } finally {
                searchCursor.close();
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    // Create a new TreeSet containing the elements satisfying the prefix search.
    // Implementing prefix search by changing compareTo() in CheckTuple does not work.
    public static TreeSet<CheckTuple> getPrefixExpectedSubset(TreeSet<CheckTuple> checkTuples, CheckTuple lowKey, CheckTuple highKey) {
        TreeSet<CheckTuple> expectedSubset = new TreeSet<CheckTuple>();
        Iterator<CheckTuple> iter = checkTuples.iterator();
        while(iter.hasNext()) {
            CheckTuple t = iter.next();
            boolean geLowKey = true;
            boolean leHighKey = true;
            for (int i = 0; i < lowKey.getNumKeys(); i++) {
                if (t.get(i).compareTo(lowKey.get(i)) < 0) {
                    geLowKey = false;
                    break;
                }
            }
            for (int i = 0; i < highKey.getNumKeys(); i++) {
                if (t.get(i).compareTo(highKey.get(i)) > 0) {
                    leHighKey = false;
                    break;
                }
            }
            if (geLowKey && leHighKey) {
                expectedSubset.add(t);
            }
        }
        return expectedSubset;
    }
    
    public static void insertIntTuples(BTreeTestContext testCtx, int numTuples, Random rnd) throws Exception {
        int numFields = testCtx.getFieldCount();
        int numKeyFields = testCtx.getKeyFieldCount();
        
        BTreeOpContext insertOpCtx = testCtx.btree.createOpContext(IndexOp.INSERT, testCtx.leafFrame, testCtx.interiorFrame, testCtx.metaFrame);
        
        int[] tupleValues = new int[testCtx.getFieldCount()];
        // Scale range of values according to number of keys. 
        // For example, for 2 keys we want the square root of numTuples, for 3 keys the cube root of numTuples, etc.        
        int maxValue = (int)Math.ceil(Math.pow(numTuples, 1.0/(double)numKeyFields));
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                tupleValues[j] = rnd.nextInt() % maxValue;
            }
            // Set values.
            for (int j = numKeyFields; j < numFields; j++) {
                tupleValues[j] = j;
            }
            TupleUtils.createIntegerTuple(testCtx.tupleBuilder, testCtx.tuple, tupleValues);
            if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
            }
            try {
                testCtx.btree.insert(testCtx.tuple, insertOpCtx);
                // Set expected values. Do this only after insertion succeeds because we ignore duplicate keys.
                CheckTuple<Integer> checkTuple = new CheckTuple<Integer>(numFields, numKeyFields);
                for(int v : tupleValues) {
                    checkTuple.add(v);
                }
                testCtx.checkTuples.add(checkTuple);
            } catch (BTreeDuplicateKeyException e) {
                // Ignore duplicate key insertions.
            }
        }
    }
    
    public static void insertStringTuples(BTreeTestContext testCtx, int numTuples, Random rnd) throws Exception {
        int numFields = testCtx.getFieldCount();
        int numKeyFields = testCtx.getKeyFieldCount();
        
        BTreeOpContext insertOpCtx = testCtx.btree.createOpContext(IndexOp.INSERT, testCtx.leafFrame, testCtx.interiorFrame, testCtx.metaFrame);
        Object[] tupleValues = new Object[numFields];
        for (int i = 0; i < numTuples; i++) {
            if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
            }
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                tupleValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < numFields; j++) {
                tupleValues[j] = getRandomString(5, rnd);
            }
            TupleUtils.createTuple(testCtx.tupleBuilder, testCtx.tuple, testCtx.fieldSerdes, tupleValues);
            try {
                testCtx.btree.insert(testCtx.tuple, insertOpCtx);
                // Set expected values. Do this only after insertion succeeds because we ignore duplicate keys.
                CheckTuple<String> checkTuple = new CheckTuple<String>(numFields, numKeyFields);
                for(Object v : tupleValues) {
                    checkTuple.add((String)v);
                }
                testCtx.checkTuples.add(checkTuple);
            } catch (BTreeDuplicateKeyException e) {
                // Ignore duplicate key insertions.
            }
        }
    }

    public static void bulkLoadIntTuples(BTreeTestContext testCtx, int numTuples, Random rnd) throws Exception {
        int numFields = testCtx.getFieldCount();
        int numKeyFields = testCtx.getKeyFieldCount();
        int[] tupleValues = new int[testCtx.getFieldCount()];
        int maxValue = (int)Math.ceil(Math.pow(numTuples, 1.0/(double)numKeyFields));
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                tupleValues[j] = rnd.nextInt() % maxValue;
            }
            // Set values.
            for (int j = numKeyFields; j < numFields; j++) {
                tupleValues[j] = j;
            }
            
            // Set expected values. We also use these as the pre-sorted stream for bulk loading.
            CheckTuple<Integer> checkTuple = new CheckTuple<Integer>(numFields, numKeyFields);
            for(int v : tupleValues) {
                checkTuple.add(v);
            }            
            testCtx.checkTuples.add(checkTuple);
        }
        
        bulkLoadCheckTuples(testCtx, numTuples);
    }
    
    public static void bulkLoadStringTuples(BTreeTestContext testCtx, int numTuples, Random rnd) throws Exception {
        int numFields = testCtx.getFieldCount();
        int numKeyFields = testCtx.getKeyFieldCount();
        String[] tupleValues = new String[numFields];
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                tupleValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < numFields; j++) {
                tupleValues[j] = getRandomString(5, rnd);
            }
            // Set expected values. We also use these as the pre-sorted stream for bulk loading.
            CheckTuple<String> checkTuple = new CheckTuple<String>(numFields, numKeyFields);
            for(String v : tupleValues) {
                checkTuple.add(v);
            }            
            testCtx.checkTuples.add(checkTuple);
        }
        
        bulkLoadCheckTuples(testCtx, numTuples);
    }
    
    private static void bulkLoadCheckTuples(BTreeTestContext testCtx, int numTuples) throws HyracksDataException, TreeIndexException {
        int numFields = testCtx.getFieldCount();
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(numFields);
        ArrayTupleReference tuple = new ArrayTupleReference();
        // Perform bulk load.
        IIndexBulkLoadContext bulkLoadCtx = testCtx.btree.beginBulkLoad(0.7f, testCtx.leafFrame, testCtx.interiorFrame, testCtx.metaFrame);
        int c = 1;
        for (CheckTuple checkTuple : testCtx.checkTuples) {
            if (c % (numTuples / 10) == 0) {
                LOGGER.info("Bulk Loading Tuple " + c + "/" + numTuples);
            }
            createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, testCtx.fieldSerdes);
            testCtx.btree.bulkLoadAddTuple(tuple, bulkLoadCtx);
            c++;
        }
        testCtx.btree.endBulkLoad(bulkLoadCtx);
    }
    
    public static void deleteTuples(BTreeTestContext testCtx, int numTuples, Random rnd) throws Exception {
        ArrayTupleBuilder deleteTupleBuilder = new ArrayTupleBuilder(testCtx.btree.getMultiComparator().getKeyFieldCount());
        ArrayTupleReference deleteTuple = new ArrayTupleReference();
        int numCheckTuples = testCtx.checkTuples.size();
        BTreeOpContext deleteOpCtx = testCtx.btree.createOpContext(IndexOp.DELETE, testCtx.leafFrame, testCtx.interiorFrame, testCtx.metaFrame);
        // Copy CheckTuple references into array, so we can randomly pick from there.
        CheckTuple[] checkTuples = new CheckTuple[numCheckTuples];
        int idx = 0;
        for (CheckTuple checkTuple : testCtx.checkTuples) {
            checkTuples[idx++] = checkTuple;
        }
        for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
            if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                LOGGER.info("Deleting Tuple " + (i + 1) + "/" + numTuples);
            }
            int checkTupleIdx = Math.abs(rnd.nextInt() % numCheckTuples);
            CheckTuple checkTuple = checkTuples[checkTupleIdx];            
            createTupleFromCheckTuple(checkTuple, deleteTupleBuilder, deleteTuple, testCtx.fieldSerdes);          
            testCtx.btree.delete(deleteTuple, deleteOpCtx);
            
            // Remove check tuple from expected results.
            testCtx.checkTuples.remove(checkTuple);
            
            // Swap with last "valid" CheckTuple.
            CheckTuple tmp = checkTuples[numCheckTuples - 1];
            checkTuples[numCheckTuples - 1] = checkTuple;
            checkTuples[checkTupleIdx] = tmp;
            numCheckTuples--;
        }
    }
    
    @SuppressWarnings("unchecked")
    public static void updateTuples(BTreeTestContext testCtx, int numTuples, Random rnd) throws Exception {
        int fieldCount = testCtx.btree.getFieldCount();
        int keyFieldCount = testCtx.btree.getMultiComparator().getKeyFieldCount();
        // This is a noop because we can only update non-key fields.
        if (fieldCount == keyFieldCount) {
            return;
        }
        ArrayTupleBuilder updateTupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference updateTuple = new ArrayTupleReference();
        int numCheckTuples = testCtx.checkTuples.size();
        BTreeOpContext updateOpCtx = testCtx.btree.createOpContext(IndexOp.UPDATE, testCtx.leafFrame, testCtx.interiorFrame, testCtx.metaFrame);
        // Copy CheckTuple references into array, so we can randomly pick from there.
        CheckTuple[] checkTuples = new CheckTuple[numCheckTuples];
        int idx = 0;
        for (CheckTuple checkTuple : testCtx.checkTuples) {
            checkTuples[idx++] = checkTuple;
        }
        for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
            if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                LOGGER.info("Updating Tuple " + (i + 1) + "/" + numTuples);
            }
            int checkTupleIdx = Math.abs(rnd.nextInt() % numCheckTuples);
            CheckTuple checkTuple = checkTuples[checkTupleIdx];
            // Update check tuple's non-key fields.
            for (int j = keyFieldCount; j < fieldCount; j++) {
                Comparable newValue = getRandomUpdateValue(testCtx.fieldSerdes[j], rnd);
                checkTuple.set(j, newValue);
            }
            
            createTupleFromCheckTuple(checkTuple, updateTupleBuilder, updateTuple, testCtx.fieldSerdes);            
            testCtx.btree.update(updateTuple, updateOpCtx);
            
            // Swap with last "valid" CheckTuple.
            CheckTuple tmp = checkTuples[numCheckTuples - 1];
            checkTuples[numCheckTuples - 1] = checkTuple;
            checkTuples[checkTupleIdx] = tmp;
            numCheckTuples--;
        }
    }
    
    private static Comparable getRandomUpdateValue(ISerializerDeserializer serde, Random rnd) {
        if (serde instanceof IntegerSerializerDeserializer) {
            return Integer.valueOf(rnd.nextInt());
        } else if (serde instanceof UTF8StringSerializerDeserializer) {
            return getRandomString(10, rnd);
        }
        return null;
    }
    
    public static String getRandomString(int length, Random rnd) {
        String s = Long.toHexString(Double.doubleToLongBits(rnd.nextDouble()));
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < s.length() && i < length; i++) {
            strBuilder.append(s.charAt(Math.abs(rnd.nextInt()) % s.length()));
        }
        return strBuilder.toString();
    }
}
