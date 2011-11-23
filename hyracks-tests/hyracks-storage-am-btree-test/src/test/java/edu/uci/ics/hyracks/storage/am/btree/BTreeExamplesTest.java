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

import java.util.Random;
import java.util.logging.Level;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.AbstractBTreeTest;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

@SuppressWarnings("rawtypes")
public class BTreeExamplesTest extends AbstractBTreeTest {

    /**
     * Fixed-Length Key,Value Example.
     * 
     * Create a BTree with one fixed-length key field and one fixed-length value
     * field. Fill BTree with random values using insertions (not bulk load).
     * Perform scans and range search.
     */
    @Test
    public void fixedLengthKeyValueExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Fixed-Length Key,Value Example.");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = ITypeTrait.INTEGER_TYPE_TRAIT;
        typeTraits[1] = ITypeTrait.INTEGER_TYPE_TRAIT;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        BTree btree = BTreeUtils
                .createBTree(bufferCache, btreeFileId, typeTraits, cmps, BTreeLeafFrameType.REGULAR_NSM);
        btree.create(btreeFileId);
        btree.open(btreeFileId);

        long start = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        int numInserts = 10000;
        for (int i = 0; i < numInserts; i++) {
            int f0 = rnd.nextInt() % numInserts;
            int f1 = 5;
            TupleUtils.createIntegerTuple(tb, tuple, f0, f1);
            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + i + " : " + f0 + " " + f1);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        orderedScan(btree, indexAccessor, fieldSerdes);
        diskOrderScan(btree, indexAccessor, fieldSerdes);

        // Build low key.
        ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(keyFieldCount);
        ArrayTupleReference lowKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(lowKeyTb, lowKey, -1000);

        // Build high key.
        ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(keyFieldCount);
        ArrayTupleReference highKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(highKeyTb, highKey, 1000);

        rangeSearch(btree, indexAccessor, fieldSerdes, lowKey, highKey);

        btree.close();
    }

    /**
     * Composite Key Example (Non-Unique B-Tree).
     * 
     * Create a BTree with two fixed-length key fields and one fixed-length
     * value field. Fill BTree with random values using insertions (not bulk
     * load) Perform scans and range search.
     */
    @Test
    public void twoFixedLengthKeysOneFixedLengthValueExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Composite Key Test");
        }

        // Declare fields.
        int fieldCount = 3;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = ITypeTrait.INTEGER_TYPE_TRAIT;
        typeTraits[1] = ITypeTrait.INTEGER_TYPE_TRAIT;
        typeTraits[2] = ITypeTrait.INTEGER_TYPE_TRAIT;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        BTree btree = BTreeUtils
                .createBTree(bufferCache, btreeFileId, typeTraits, cmps, BTreeLeafFrameType.REGULAR_NSM);
        btree.create(btreeFileId);
        btree.open(btreeFileId);

        long start = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        int numInserts = 10000;
        for (int i = 0; i < 10000; i++) {
            int f0 = rnd.nextInt() % 2000;
            int f1 = rnd.nextInt() % 1000;
            int f2 = 5;
            TupleUtils.createIntegerTuple(tb, tuple, f0, f1, f2);
            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + i + " : " + f0 + " " + f1 + " " + f2);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        orderedScan(btree, indexAccessor, fieldSerdes);
        diskOrderScan(btree, indexAccessor, fieldSerdes);

        // Build low key.
        ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference lowKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(lowKeyTb, lowKey, -3);

        // Build high key.
        ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference highKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(highKeyTb, highKey, 3);

        // Prefix-Range search in [-3, 3]
        rangeSearch(btree, indexAccessor, fieldSerdes, lowKey, highKey);

        btree.close();
    }

    /**
     * Variable-Length Example. Create a BTree with one variable-length key
     * field and one variable-length value field. Fill BTree with random values
     * using insertions (not bulk load) Perform ordered scans and range search.
     */
    @Test
    public void varLenKeyValueExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Variable-Length Key,Value Example");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = ITypeTrait.VARLEN_TYPE_TRAIT;
        typeTraits[1] = ITypeTrait.VARLEN_TYPE_TRAIT;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        BTree btree = BTreeUtils
                .createBTree(bufferCache, btreeFileId, typeTraits, cmps, BTreeLeafFrameType.REGULAR_NSM);
        btree.create(btreeFileId);
        btree.open(btreeFileId);

        long start = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        // Max string length to be generated.
        int maxLength = 10;
        int numInserts = 10000;
        for (int i = 0; i < 10000; i++) {
            String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            TupleUtils.createTuple(tb, tuple, fieldSerdes, f0, f1);
            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + f0 + " " + f1);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        orderedScan(btree, indexAccessor, fieldSerdes);
        diskOrderScan(btree, indexAccessor, fieldSerdes);

        // Build low key.
        ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference lowKey = new ArrayTupleReference();
        TupleUtils.createTuple(lowKeyTb, lowKey, fieldSerdes, "cbf");

        // Build high key.
        ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference highKey = new ArrayTupleReference();
        TupleUtils.createTuple(highKeyTb, highKey, fieldSerdes, "cc7");

        rangeSearch(btree, indexAccessor, fieldSerdes, lowKey, highKey);

        btree.close();
    }

    /**
     * Deletion Example.
     * 
     * Create a BTree with one variable-length key field and one variable-length
     * value field. Fill B-tree with random values using insertions, then delete
     * entries one-by-one. Repeat procedure a few times on same BTree.
     */
    @Test
    public void deleteExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Deletion Example");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = ITypeTrait.VARLEN_TYPE_TRAIT;
        typeTraits[1] = ITypeTrait.VARLEN_TYPE_TRAIT;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        BTree btree = BTreeUtils
                .createBTree(bufferCache, btreeFileId, typeTraits, cmps, BTreeLeafFrameType.REGULAR_NSM);
        btree.create(btreeFileId);
        btree.open(btreeFileId);

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        // Max string length to be generated.
        int runs = 3;
        for (int run = 0; run < runs; run++) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deletion example run: " + (run + 1) + "/" + runs);
                LOGGER.info("Inserting into tree...");
            }
            int maxLength = 10;
            int ins = 10000;
            String[] f0s = new String[ins];
            String[] f1s = new String[ins];
            int insDone = 0;
            int[] insDoneCmp = new int[ins];
            for (int i = 0; i < ins; i++) {
                String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                TupleUtils.createTuple(tb, tuple, fieldSerdes, f0, f1);
                f0s[i] = f0;
                f1s[i] = f1;
                if (LOGGER.isLoggable(Level.INFO)) {
                    if (i % 1000 == 0) {
                        LOGGER.info("Inserting " + i);
                    }
                }
                try {
                    indexAccessor.insert(tuple);
                    insDone++;
                } catch (TreeIndexException e) {
                }
                insDoneCmp[i] = insDone;
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deleting from tree...");
            }
            int delDone = 0;
            for (int i = 0; i < ins; i++) {
                TupleUtils.createTuple(tb, tuple, fieldSerdes, f0s[i], f1s[i]);
                if (LOGGER.isLoggable(Level.INFO)) {
                    if (i % 1000 == 0) {
                        LOGGER.info("Deleting " + i);
                    }
                }
                try {
                    indexAccessor.delete(tuple);
                    delDone++;
                } catch (TreeIndexException e) {
                }
                if (insDoneCmp[i] != delDone) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("INCONSISTENT STATE, ERROR IN DELETION EXAMPLE.");
                        LOGGER.info("INSDONECMP: " + insDoneCmp[i] + " " + delDone);
                    }
                    break;
                }
            }
            if (insDone != delDone) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("ERROR! INSDONE: " + insDone + " DELDONE: " + delDone);
                }
                break;
            }
        }
        btree.close();
    }

    /**
     * Update example.
     * 
     * Create a BTree with one variable-length key field and one variable-length
     * value field. Fill B-tree with random values using insertions, then update
     * entries one-by-one. Repeat procedure a few times on same BTree.
     */
    @Test
    public void updateExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Update example");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = ITypeTrait.VARLEN_TYPE_TRAIT;
        typeTraits[1] = ITypeTrait.VARLEN_TYPE_TRAIT;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        BTree btree = BTreeUtils
                .createBTree(bufferCache, btreeFileId, typeTraits, cmps, BTreeLeafFrameType.REGULAR_NSM);
        btree.create(btreeFileId);
        btree.open(btreeFileId);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inserting into tree...");
        }
        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        int maxLength = 10;
        int ins = 10000;
        String[] keys = new String[10000];
        for (int i = 0; i < ins; i++) {
            String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            TupleUtils.createTuple(tb, tuple, fieldSerdes, f0, f1);
            keys[i] = f0;
            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + i);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            }
        }
        // Print before doing any updates.
        orderedScan(btree, indexAccessor, fieldSerdes);

        int runs = 3;
        for (int run = 0; run < runs; run++) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Update test run: " + (run + 1) + "/" + runs);
                LOGGER.info("Updating BTree");
            }
            for (int i = 0; i < ins; i++) {
                // Generate a new random value for f1.
                String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                TupleUtils.createTuple(tb, tuple, fieldSerdes, keys[i], f1);
                if (LOGGER.isLoggable(Level.INFO)) {
                    if (i % 1000 == 0) {
                        LOGGER.info("UPDATING " + i);
                    }
                }
                try {
                    indexAccessor.update(tuple);
                } catch (TreeIndexException e) {
                }
            }
            // Do another scan after a round of updates.
            orderedScan(btree, indexAccessor, fieldSerdes);
        }
        btree.close();
    }

    /**
     * Bulk load example.
     * 
     * Load a tree with 100,000 tuples. BTree has a composite key to "simulate"
     * non-unique index creation.
     * 
     */
    @Test
    public void bulkLoadExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Bulk load example");
        }
        // Declare fields.
        int fieldCount = 3;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = ITypeTrait.INTEGER_TYPE_TRAIT;
        typeTraits[1] = ITypeTrait.INTEGER_TYPE_TRAIT;
        typeTraits[2] = ITypeTrait.INTEGER_TYPE_TRAIT;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        BTree btree = BTreeUtils
                .createBTree(bufferCache, btreeFileId, typeTraits, cmps, BTreeLeafFrameType.REGULAR_NSM);
        btree.create(btreeFileId);
        btree.open(btreeFileId);

        // Load sorted records.
        int ins = 100000;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Bulk loading " + ins + " tuples");
        }
        long start = System.currentTimeMillis();
        IIndexBulkLoadContext bulkLoadCtx = btree.beginBulkLoad(0.7f);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        for (int i = 0; i < ins; i++) {
            TupleUtils.createIntegerTuple(tb, tuple, i, i, 5);
            btree.bulkLoadAddTuple(tuple, bulkLoadCtx);
        }
        btree.endBulkLoad(bulkLoadCtx);
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(ins + " tuples loaded in " + (end - start) + "ms");
        }

        ITreeIndexAccessor indexAccessor = btree.createAccessor();

        // Build low key.
        ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference lowKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(lowKeyTb, lowKey, 44444);

        // Build high key.
        ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference highKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(highKeyTb, highKey, 44500);

        // Prefix-Range search in [44444, 44500]
        rangeSearch(btree, indexAccessor, fieldSerdes, lowKey, highKey);

        btree.close();
    }

    private void orderedScan(BTree btree, ITreeIndexAccessor indexAccessor, ISerializerDeserializer[] fieldSerdes)
            throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Ordered Scan:");
        }
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame, false);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        indexAccessor.search(scanCursor, nullPred);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(rec);
                }
            }
        } finally {
            scanCursor.close();
        }
    }

    private void diskOrderScan(BTree btree, ITreeIndexAccessor indexAccessor, ISerializerDeserializer[] fieldSerdes)
            throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Disk-Order Scan:");
        }
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        TreeDiskOrderScanCursor diskOrderCursor = new TreeDiskOrderScanCursor(leafFrame);
        indexAccessor.diskOrderScan(diskOrderCursor);
        try {
            while (diskOrderCursor.hasNext()) {
                diskOrderCursor.next();
                ITupleReference frameTuple = diskOrderCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(rec);
                }
            }
        } finally {
            diskOrderCursor.close();
        }
    }

    private void rangeSearch(BTree btree, ITreeIndexAccessor indexAccessor, ISerializerDeserializer[] fieldSerdes,
            ITupleReference lowKey, ITupleReference highKey) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            String lowKeyString = TupleUtils.printTuple(lowKey, fieldSerdes);
            String highKeyString = TupleUtils.printTuple(highKey, fieldSerdes);
            LOGGER.info("Range-Search in: [" + lowKeyString + ", " + highKeyString + "]");
        }
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        MultiComparator lowKeySearchCmp = BTreeUtils.getSearchMultiComparator(btree.getMultiComparator(), lowKey);
        MultiComparator highKeySearchCmp = BTreeUtils.getSearchMultiComparator(btree.getMultiComparator(), highKey);
        ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame, false);
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, lowKeySearchCmp,
                highKeySearchCmp);
        indexAccessor.search(rangeCursor, rangePred);
        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(rec);
                }
            }
        } finally {
            rangeCursor.close();
        }
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