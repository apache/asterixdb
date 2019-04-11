/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.btree;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.btree.util.AbstractBTreeTest;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrameFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.junit.Test;

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
        cmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        BTreeTypeAwareTupleWriterFactory tupleWriterFactory = new BTreeTypeAwareTupleWriterFactory(typeTraits, false);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetadataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();

        IMetadataPageManager freePageManager = new LinkedMetaDataPageManager(bufferCache, metaFrameFactory);
        BTree btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                fieldCount, harness.getFileReference());
        btree.create();
        btree.activate();

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("INSERTING INTO TREE");
        }

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference insertTuple = new ArrayTupleReference();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        ITreeIndexAccessor indexAccessor = btree.createAccessor(actx);
        try {
            int numInserts = 10000;
            for (int i = 0; i < numInserts; i++) {
                int f0 = rnd.nextInt() % 10000;
                int f1 = 5;
                TupleUtils.createIntegerTuple(tb, insertTuple, f0, f1);
                if (LOGGER.isInfoEnabled()) {
                    if (i % 10000 == 0) {
                        long end = System.currentTimeMillis();
                        LOGGER.info("INSERTING " + i + " : " + f0 + " " + f1 + " " + (end - start));
                    }
                }

                try {
                    indexAccessor.insert(insertTuple);
                } catch (HyracksDataException hde) {
                    if (hde.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                        hde.printStackTrace();
                        throw hde;
                    }
                }
            }
            long end = System.currentTimeMillis();
            long duration = end - start;
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("DURATION: " + duration);
            }

            // Update scan.
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("UPDATE SCAN:");
            }
            // Set the cursor to X latch nodes.
            RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
            ITreeIndexCursor updateScanCursor = new BTreeRangeSearchCursor(leafFrame, true);
            try {
                indexAccessor.search(updateScanCursor, nullPred);
                try {
                    while (updateScanCursor.hasNext()) {
                        updateScanCursor.next();
                        ITupleReference tuple = updateScanCursor.getTuple();
                        // Change the value field.
                        IntegerPointable.setInteger(tuple.getFieldData(1), tuple.getFieldStart(1), 10);
                    }
                } finally {
                    updateScanCursor.close();
                }
            } finally {
                updateScanCursor.destroy();
            }
            // Ordered scan to verify the values.
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("ORDERED SCAN:");
            }
            // Set the cursor to X latch nodes.
            ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame, true);
            try {
                indexAccessor.search(scanCursor, nullPred);
                try {
                    while (scanCursor.hasNext()) {
                        scanCursor.next();
                        ITupleReference tuple = scanCursor.getTuple();
                        String rec = TupleUtils.printTuple(tuple, recDescSers);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(rec);
                        }
                    }
                } finally {
                    scanCursor.close();
                }
            } finally {
                scanCursor.destroy();
            }
        } finally {
            indexAccessor.destroy();
        }
        btree.deactivate();
        btree.destroy();
    }
}
