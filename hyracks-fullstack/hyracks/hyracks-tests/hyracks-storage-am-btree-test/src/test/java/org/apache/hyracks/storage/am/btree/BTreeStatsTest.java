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

import java.io.DataOutput;
import java.util.Random;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.btree.util.AbstractBTreeTest;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrameFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.util.TreeIndexBufferCacheWarmup;
import org.apache.hyracks.storage.am.common.util.TreeIndexStats;
import org.apache.hyracks.storage.am.common.util.TreeIndexStatsGatherer;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class BTreeStatsTest extends AbstractBTreeTest {
    private static final int PAGE_SIZE = 4096;
    private static final int NUM_PAGES = 1000;
    private static final int MAX_OPEN_FILES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;

    private final IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    @Test
    public void test01() throws Exception {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
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
        BTreeTypeAwareTupleWriterFactory tupleWriterFactory = new BTreeTypeAwareTupleWriterFactory(typeTraits, false);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetadataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        ITreeIndexMetadataFrame metaFrame = metaFrameFactory.createFrame();
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
        IFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();
        ISerializerDeserializer[] recDescSers =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(recDesc);
        accessor.reset(frame.getBuffer());
        FrameTupleReference tuple = new FrameTupleReference();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        ITreeIndexAccessor indexAccessor = btree.createAccessor(actx);
        // 10000
        for (int i = 0; i < 100000; i++) {
            int f0 = rnd.nextInt() % 100000;
            int f1 = 5;
            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();
            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            tuple.reset(accessor, 0);
            if (LOGGER.isInfoEnabled()) {
                if (i % 10000 == 0) {
                    long end = System.currentTimeMillis();
                    LOGGER.info("INSERTING " + i + " : " + f0 + " " + f1 + " " + (end - start));
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    e.printStackTrace();
                    throw e;
                }
            }
        }
        TreeIndexStatsGatherer statsGatherer = new TreeIndexStatsGatherer(bufferCache, freePageManager,
                harness.getFileReference(), btree.getRootPageId());
        TreeIndexStats stats = statsGatherer.gatherStats(leafFrame, interiorFrame, metaFrame);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("\n" + stats.toString());
        }
        TreeIndexBufferCacheWarmup bufferCacheWarmup =
                new TreeIndexBufferCacheWarmup(bufferCache, freePageManager, harness.getFileReference());
        bufferCacheWarmup.warmup(leafFrame, metaFrame, new int[] { 1, 2 }, new int[] { 2, 5 });
        btree.deactivate();
        btree.destroy();
        bufferCache.close();
    }
}
