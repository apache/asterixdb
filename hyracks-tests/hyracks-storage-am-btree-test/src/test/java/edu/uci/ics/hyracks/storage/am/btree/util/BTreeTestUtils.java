package edu.uci.ics.hyracks.storage.am.btree.util;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public class BTreeTestUtils {
    private static final Logger LOGGER = Logger.getLogger(BTreeTestUtils.class.getName());
    private static final long RANDOM_SEED = 50;
    
    public static BTree createBTree(IBufferCache bufferCache, int btreeFileId, ITypeTrait[] typeTraits, IBinaryComparator[] cmps) {
        MultiComparator cmp = new MultiComparator(typeTraits, cmps);
        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);        
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, btreeFileId, 0, metaFrameFactory);
        BTree btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmp);
        return btree;
    }
    
    public static BTreeTestContext createBTreeTestContext(IBufferCache bufferCache, int btreeFileId, ISerializerDeserializer[] fieldSerdes, int numKeyFields) throws Exception {        
        ITypeTrait[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes, fieldSerdes.length);
        IBinaryComparator[] cmps = SerdeUtils.serdesToComparators(fieldSerdes, numKeyFields);
        
        BTree btree = BTreeTestUtils.createBTree(bufferCache, btreeFileId, typeTraits, cmps);
        
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
    private static CheckTuple createCheckTupleFromTuple(ITupleReference tuple, int numKeys, ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
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
    
    public static void checkOrderedScan(BTreeTestContext testCtx) throws Exception {
        LOGGER.info("Testing Ordered Scan:");
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
        LOGGER.info("Testing Disk-Order Scan:");
        ITreeIndexCursor diskOrderCursor = new TreeDiskOrderScanCursor(testCtx.leafFrame);
        BTreeOpContext diskOrderScanOpCtx = testCtx.btree.createOpContext(IndexOp.DISKORDERSCAN, testCtx.leafFrame, null, null);
        testCtx.btree.diskOrderScan(diskOrderCursor, testCtx.leafFrame, testCtx.metaFrame, diskOrderScanOpCtx);
        int actualCount = 0;        
        try {
            while (diskOrderCursor.hasNext()) {
                diskOrderCursor.next();
                ITupleReference tuple = diskOrderCursor.getTuple();
                CheckTuple checkTuple = createCheckTupleFromTuple(tuple, testCtx.btree.getMultiComparator().getKeyFieldCount(), testCtx.fieldSerdes);
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
        LOGGER.info("Testing Range Search:");
        MultiComparator lowKeyCmp = getSearchMultiComparator(testCtx.btree.getMultiComparator(), lowKey);
        MultiComparator highKeyCmp = getSearchMultiComparator(testCtx.btree.getMultiComparator(), highKey);
        ITreeIndexCursor searchCursor = new BTreeRangeSearchCursor(testCtx.leafFrame);
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, lowKeyInclusive, highKeyInclusive, lowKeyCmp, highKeyCmp);
        BTreeOpContext searchOpCtx = testCtx.btree.createOpContext(IndexOp.SEARCH, testCtx.leafFrame, testCtx.interiorFrame, null);
        testCtx.btree.search(searchCursor, rangePred, searchOpCtx);
        // Get the subset of elements from the expected set within given key range.
        CheckTuple lowKeyCheck = createCheckTupleFromTuple(lowKey, lowKeyCmp.getKeyFieldCount(), testCtx.fieldSerdes);
        CheckTuple highKeyCheck = createCheckTupleFromTuple(highKey, highKeyCmp.getKeyFieldCount(), testCtx.fieldSerdes);
        NavigableSet<CheckTuple> expectedSubset = null;
        if (lowKeyCmp.getKeyFieldCount() < testCtx.btree.getMultiComparator().getKeyFieldCount() || 
                highKeyCmp.getKeyFieldCount() < testCtx.btree.getMultiComparator().getKeyFieldCount()) {
            // Searching on a key prefix (on low key or high key or both).
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
    
    public static MultiComparator getSearchMultiComparator(MultiComparator btreeCmp, ITupleReference searchKey) {
        if (btreeCmp.getKeyFieldCount() == searchKey.getFieldCount()) {
            return btreeCmp;
        }
        IBinaryComparator[] cmps = new IBinaryComparator[searchKey.getFieldCount()];
        for (int i = 0; i < searchKey.getFieldCount(); i++) {
            cmps[i] = btreeCmp.getComparators()[i];
        }
        return new MultiComparator(btreeCmp.getTypeTraits(), cmps);
    }
    
    private static String printCursorResults(BTree btree, ITreeIndexCursor cursor, ISerializerDeserializer[] fieldSerdes) throws HyracksDataException, Exception {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("\n");
        while (cursor.hasNext()) {
            cursor.next();
            ITupleReference frameTuple = cursor.getTuple();
            String tupleString = btree.getMultiComparator().printTuple(frameTuple, fieldSerdes);
            strBuilder.append(tupleString + "\n");
        }
        return strBuilder.toString();
    }
    
    public static void fillIntBTree(BTreeTestContext testCtx, int numTuples) throws Exception {
        int numFields = testCtx.getFieldCount();
        int numKeyFields = testCtx.getKeyFieldCount();
        
        Random rnd = new Random();
        rnd.setSeed(RANDOM_SEED);
        
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
            if ((i + 1) % (numTuples / 10) == 0) {
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
    
    public static void fillStringBTree(BTreeTestContext testCtx, int numTuples) throws Exception {
        int numFields = testCtx.getFieldCount();
        int numKeyFields = testCtx.getKeyFieldCount();
        
        BTreeOpContext insertOpCtx = testCtx.btree.createOpContext(IndexOp.INSERT, testCtx.leafFrame, testCtx.interiorFrame, testCtx.metaFrame);
        Random rnd = new Random();
        rnd.setSeed(RANDOM_SEED);
        Object[] tupleValues = new Object[numFields];
        for (int i = 0; i < numTuples; i++) {
            if ((i + 1) % (numTuples / 10) == 0) {
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

    public static String getRandomString(int length, Random rnd) {
        String s = Long.toHexString(Double.doubleToLongBits(rnd.nextDouble()));
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < s.length() && i < length; i++) {
            strBuilder.append(s.charAt(Math.abs(rnd.nextInt()) % s.length()));
        }
        return strBuilder.toString();
    }
}
