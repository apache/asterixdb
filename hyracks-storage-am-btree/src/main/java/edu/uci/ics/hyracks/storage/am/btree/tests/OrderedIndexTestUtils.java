package edu.uci.ics.hyracks.storage.am.btree.tests;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

@SuppressWarnings("rawtypes")
public class OrderedIndexTestUtils {
	private static final Logger LOGGER = Logger.getLogger(OrderedIndexTestUtils.class.getName());
	
	private static void compareActualAndExpected(ITupleReference actual, CheckTuple expected, ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
        for (int i = 0; i < fieldSerdes.length; i++) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(
                    actual.getFieldData(i), actual.getFieldStart(i),
                    actual.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Object actualObj = fieldSerdes[i].deserialize(dataIn);            
            if (!actualObj.equals(expected.get(i))) {
                fail("Actual and expected fields do not match on field " + i + ".\nExpected: " + expected.get(i) + "\nActual  : " + actualObj);
            }
        }
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
	
	@SuppressWarnings("unchecked")
    private static CheckTuple createCheckTupleFromTuple(ITupleReference tuple, ISerializerDeserializer[] fieldSerdes, int numKeys) throws HyracksDataException {
        CheckTuple checkTuple = new CheckTuple(fieldSerdes.length, numKeys);
        int fieldCount = Math.min(fieldSerdes.length, tuple.getFieldCount());
        for (int i = 0; i < fieldCount; i++) {
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
	
	public static void checkOrderedScan(IOrderedIndexTestContext ctx) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing Ordered Scan.");
        }
        ITreeIndexCursor scanCursor = ctx.getIndexAccessor().createSearchCursor();
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        ctx.getIndexAccessor().search(scanCursor, nullPred);
        Iterator<CheckTuple> checkIter = ctx.getCheckTuples().iterator();
        int actualCount = 0;
        try {
            while (scanCursor.hasNext()) {
                if (!checkIter.hasNext()) {
                    fail("Ordered scan returned more answers than expected.\nExpected: " + ctx.getCheckTuples().size());
                }
                scanCursor.next();
                CheckTuple expectedTuple = checkIter.next();
                ITupleReference tuple = scanCursor.getTuple();
                compareActualAndExpected(tuple, expectedTuple, ctx.getFieldSerdes());
                actualCount++;
            }
            if (actualCount < ctx.getCheckTuples().size()) {
                fail("Ordered scan returned fewer answers than expected.\nExpected: " + ctx.getCheckTuples().size() + "\nActual  : " + actualCount);
            }
        } finally {
            scanCursor.close();
        }
    }
	
    public static void checkDiskOrderScan(IOrderedIndexTestContext ctx) throws Exception {
        try {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Testing Disk-Order Scan.");
            }
            ITreeIndexCursor diskOrderCursor = ctx.getIndexAccessor().createDiskOrderScanCursor();
            ctx.getIndexAccessor().diskOrderScan(diskOrderCursor);
            int actualCount = 0;
            try {
                while (diskOrderCursor.hasNext()) {
                    diskOrderCursor.next();
                    ITupleReference tuple = diskOrderCursor.getTuple();
                    CheckTuple checkTuple = createCheckTupleFromTuple(tuple, ctx.getFieldSerdes(), ctx.getKeyFieldCount());
                    if (!ctx.getCheckTuples().contains(checkTuple)) {
                        fail("Disk-order scan returned unexpected answer: " + checkTuple.toString());
                    }
                    actualCount++;
                }
                if (actualCount < ctx.getCheckTuples().size()) {
                    fail("Disk-order scan returned fewer answers than expected.\nExpected: "
                            + ctx.getCheckTuples().size() + "\nActual  : " + actualCount);
                }
                if (actualCount > ctx.getCheckTuples().size()) {
                    fail("Disk-order scan returned more answers than expected.\nExpected: "
                            + ctx.getCheckTuples().size() + "\nActual  : " + actualCount);
                }
            } finally {
                diskOrderCursor.close();
            }
        } catch (UnsupportedOperationException e) {
            // Ignore exception because some indexes, e.g. the LSMBTree, don't
            // support disk-order scan.
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        }
    }
    
    public static void checkRangeSearch(IOrderedIndexTestContext ctx, ITupleReference lowKey, ITupleReference highKey, boolean lowKeyInclusive, boolean highKeyInclusive) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing Range Search.");
        }
        MultiComparator lowKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), lowKey);
        MultiComparator highKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), highKey);
        ITreeIndexCursor searchCursor = ctx.getIndexAccessor().createSearchCursor();
        RangePredicate rangePred = new RangePredicate(lowKey, highKey, lowKeyInclusive, highKeyInclusive, lowKeyCmp, highKeyCmp);
        ctx.getIndexAccessor().search(searchCursor, rangePred);
        // Get the subset of elements from the expected set within given key range.
        CheckTuple lowKeyCheck = createCheckTupleFromTuple(lowKey, ctx.getFieldSerdes(), lowKeyCmp.getKeyFieldCount());
        CheckTuple highKeyCheck = createCheckTupleFromTuple(highKey, ctx.getFieldSerdes(), highKeyCmp.getKeyFieldCount());
        NavigableSet<CheckTuple> expectedSubset = null;
        if (lowKeyCmp.getKeyFieldCount() < ctx.getKeyFieldCount() || 
                highKeyCmp.getKeyFieldCount() < ctx.getKeyFieldCount()) {
            // Searching on a key prefix (low key or high key or both).
            expectedSubset = getPrefixExpectedSubset(ctx.getCheckTuples(), lowKeyCheck, highKeyCheck);
        } else {
            // Searching on all key fields.
            expectedSubset = ctx.getCheckTuples().subSet(lowKeyCheck, lowKeyInclusive, highKeyCheck, highKeyInclusive);
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
                compareActualAndExpected(tuple, expectedTuple, ctx.getFieldSerdes());
                actualCount++;
            }
            if (actualCount < expectedSubset.size()) {
                fail("Range search returned fewer answers than expected.\nExpected: " + expectedSubset.size() + "\nActual  : " + actualCount);
            }
        } finally {
            searchCursor.close();
        }
    }
	
    public static void checkPointSearches(IOrderedIndexTestContext ctx) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing Point Searches On All Expected Keys.");
        }        
        
        ITreeIndexCursor searchCursor = ctx.getIndexAccessor().createSearchCursor();
        
        ArrayTupleBuilder lowKeyBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
        ArrayTupleReference lowKey = new ArrayTupleReference();
        ArrayTupleBuilder highKeyBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
        ArrayTupleReference highKey = new ArrayTupleReference();
        RangePredicate rangePred = new RangePredicate(lowKey, highKey, true, true, null, null);

        // Iterate through expected tuples, and perform a point search in the BTree to verify the tuple can be reached.
        for (CheckTuple checkTuple : ctx.getCheckTuples()) {
            createTupleFromCheckTuple(checkTuple, lowKeyBuilder, lowKey, ctx.getFieldSerdes());
            createTupleFromCheckTuple(checkTuple, highKeyBuilder, highKey, ctx.getFieldSerdes());
            MultiComparator lowKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), lowKey);
            MultiComparator highKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), highKey);
                        
            rangePred.setLowKey(lowKey, true);
            rangePred.setHighKey(highKey, true);
            rangePred.setLowKeyComparator(lowKeyCmp);
            rangePred.setHighKeyComparator(highKeyCmp);
            
            ctx.getIndexAccessor().search(searchCursor, rangePred);
            
            try {
                // We expect exactly one answer.
                if (searchCursor.hasNext()) {
                    searchCursor.next();
                    ITupleReference tuple = searchCursor.getTuple();
                    compareActualAndExpected(tuple, checkTuple, ctx.getFieldSerdes());
                }
                if (searchCursor.hasNext()) {
                    fail("Point search returned more than one answer.");
                }
            } finally {
                searchCursor.close();
            }
        }
    }
    
	public static void insertIntTuples(IOrderedIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        // Scale range of values according to number of keys. 
        // For example, for 2 keys we want the square root of numTuples, for 3 keys the cube root of numTuples, etc.        
        int maxValue = (int)Math.ceil(Math.pow(numTuples, 1.0/(double)numKeyFields));
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                fieldValues[j] = rnd.nextInt() % maxValue;
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = j;
            }
            TupleUtils.createIntegerTuple(ctx.getTupleBuilder(), ctx.getTuple(), fieldValues);
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            try {
            	ctx.getIndexAccessor().insert(ctx.getTuple());
                // Set expected values. Do this only after insertion succeeds because we ignore duplicate keys.
                ctx.insertCheckTuple(ctx.createIntCheckTuple(fieldValues), ctx.getCheckTuples());
            } catch (BTreeDuplicateKeyException e) {
                // Ignore duplicate key insertions.
            }                        
        }
    }
	
	public static void insertStringTuples(IOrderedIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        String[] fieldValues = new String[fieldCount];
        for (int i = 0; i < numTuples; i++) {
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                fieldValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = getRandomString(5, rnd);
            }
            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), (Object[])fieldValues);
            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());
                // Set expected values. Do this only after insertion succeeds because we ignore duplicate keys.
                ctx.insertCheckTuple(ctx.createStringCheckTuple(fieldValues), ctx.getCheckTuples());
            } catch (BTreeDuplicateKeyException e) {
                // Ignore duplicate key insertions.
            }
        }
    }
	
	public static void bulkLoadIntTuples(IOrderedIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        int maxValue = (int)Math.ceil(Math.pow(numTuples, 1.0/(double)numKeyFields));
        TreeSet<CheckTuple> tmpCheckTuples = new TreeSet<CheckTuple>();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                fieldValues[j] = rnd.nextInt() % maxValue;
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = j;
            }
            
            // Set expected values. We also use these as the pre-sorted stream for bulk loading.
            ctx.insertCheckTuple(ctx.createIntCheckTuple(fieldValues), tmpCheckTuples);
        }
        bulkLoadCheckTuples(ctx, tmpCheckTuples);
        
        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }
    
    public static void bulkLoadStringTuples(IOrderedIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        String[] fieldValues = new String[fieldCount];
        TreeSet<CheckTuple> tmpCheckTuples = new TreeSet<CheckTuple>();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                fieldValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = getRandomString(5, rnd);
            }
            // Set expected values. We also use these as the pre-sorted stream for bulk loading.
            ctx.insertCheckTuple(ctx.createStringCheckTuple(fieldValues), tmpCheckTuples);
        }
        bulkLoadCheckTuples(ctx, tmpCheckTuples);
        
        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }
    
    private static void bulkLoadCheckTuples(IOrderedIndexTestContext ctx, TreeSet<CheckTuple> checkTuples) throws HyracksDataException, TreeIndexException {
        int fieldCount = ctx.getFieldCount();
        int numTuples = checkTuples.size();
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        // Perform bulk load.
        IIndexBulkLoadContext bulkLoadCtx = ctx.getIndex().beginBulkLoad(0.7f);
        int c = 1;
        for (CheckTuple checkTuple : checkTuples) {
            if (LOGGER.isLoggable(Level.INFO)) {
                if (c % (numTuples / 10) == 0) {
                    LOGGER.info("Bulk Loading Tuple " + c + "/" + numTuples);
                }
            }
            createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, ctx.getFieldSerdes());
            ctx.getIndex().bulkLoadAddTuple(tuple, bulkLoadCtx);
            c++;
        }
        ctx.getIndex().endBulkLoad(bulkLoadCtx);
    }
    
    public static void deleteTuples(IOrderedIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        ArrayTupleBuilder deleteTupleBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
        ArrayTupleReference deleteTuple = new ArrayTupleReference();
        int numCheckTuples = ctx.getCheckTuples().size();        
        // Copy CheckTuple references into array, so we can randomly pick from there.
        CheckTuple[] checkTuples = new CheckTuple[numCheckTuples];
        int idx = 0;
        for (CheckTuple checkTuple : ctx.getCheckTuples()) {
            checkTuples[idx++] = checkTuple;
        }
        for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Deleting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            int checkTupleIdx = Math.abs(rnd.nextInt() % numCheckTuples);
            CheckTuple checkTuple = checkTuples[checkTupleIdx];            
            createTupleFromCheckTuple(checkTuple, deleteTupleBuilder, deleteTuple, ctx.getFieldSerdes());
            ctx.getIndexAccessor().delete(deleteTuple);
            
            // Remove check tuple from expected results.
            ctx.getCheckTuples().remove(checkTuple);
            
            // Swap with last "valid" CheckTuple.
            CheckTuple tmp = checkTuples[numCheckTuples - 1];
            checkTuples[numCheckTuples - 1] = checkTuple;
            checkTuples[checkTupleIdx] = tmp;
            numCheckTuples--;                        
        }
    }
    
    @SuppressWarnings("unchecked")
    public static void updateTuples(IOrderedIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int keyFieldCount = ctx.getKeyFieldCount();
        // This is a noop because we can only update non-key fields.
        if (fieldCount == keyFieldCount) {
            return;
        }
        ArrayTupleBuilder updateTupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference updateTuple = new ArrayTupleReference();
        int numCheckTuples = ctx.getCheckTuples().size();
        // Copy CheckTuple references into array, so we can randomly pick from there.
        CheckTuple[] checkTuples = new CheckTuple[numCheckTuples];
        int idx = 0;
        for (CheckTuple checkTuple : ctx.getCheckTuples()) {
            checkTuples[idx++] = checkTuple;
        }
        for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Updating Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            int checkTupleIdx = Math.abs(rnd.nextInt() % numCheckTuples);
            CheckTuple checkTuple = checkTuples[checkTupleIdx];
            // Update check tuple's non-key fields.
            for (int j = keyFieldCount; j < fieldCount; j++) {
                Comparable newValue = getRandomUpdateValue(ctx.getFieldSerdes()[j], rnd);
                checkTuple.set(j, newValue);
            }
            
            createTupleFromCheckTuple(checkTuple, updateTupleBuilder, updateTuple, ctx.getFieldSerdes());            
            ctx.getIndexAccessor().update(updateTuple);
            
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
