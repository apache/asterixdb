/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.AbstractBTreeTest;
import edu.uci.ics.hyracks.storage.am.common.TestOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class BTreeUpdateSearchTest extends AbstractBTreeTest {

    // Update scan test on fixed-length tuples.
    @Test
    public void test01() throws Exception {
        IBufferCache bufferCache = harness.getBufferCache();

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, 0, metaFrameFactory);
        BTree btree = new BTree(bufferCache, harness.getFileMapProvider(), freePageManager, interiorFrameFactory,
                leafFrameFactory, cmpFactories, fieldCount, harness.getFileReference());
        btree.create();
        btree.activate();

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("INSERTING INTO TREE");
        }

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference insertTuple = new ArrayTupleReference();
        ITreeIndexAccessor indexAccessor = btree.createAccessor(TestOperationCallback.INSTANCE,
                TestOperationCallback.INSTANCE);

        int numInserts = 10000;
        for (int i = 0; i < numInserts; i++) {
            int f0 = rnd.nextInt() % 10000;
            int f1 = 5;
            TupleUtils.createIntegerTuple(tb, insertTuple, f0, f1);
            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 10000 == 0) {
                    long end = System.currentTimeMillis();
                    LOGGER.info("INSERTING " + i + " : " + f0 + " " + f1 + " " + (end - start));
                }
            }

            try {
                indexAccessor.insert(insertTuple);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("DURATION: " + duration);
        }

        // Update scan.
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("UPDATE SCAN:");
        }
        // Set the cursor to X latch nodes.
        ITreeIndexCursor updateScanCursor = new BTreeRangeSearchCursor(leafFrame, true);
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        indexAccessor.search(updateScanCursor, nullPred);
        try {
            while (updateScanCursor.hasNext()) {
                updateScanCursor.next();
                ITupleReference tuple = updateScanCursor.getTuple();
                // Change the value field.
                IntegerSerializerDeserializer.putInt(10, tuple.getFieldData(1), tuple.getFieldStart(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            updateScanCursor.close();
        }

        // Ordered scan to verify the values.
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("ORDERED SCAN:");
        }
        // Set the cursor to X latch nodes.
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame, true);
        indexAccessor.search(scanCursor, nullPred);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference tuple = scanCursor.getTuple();
                String rec = TupleUtils.printTuple(tuple, recDescSers);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(rec);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }
        btree.deactivate();
        btree.destroy();
    }
}
