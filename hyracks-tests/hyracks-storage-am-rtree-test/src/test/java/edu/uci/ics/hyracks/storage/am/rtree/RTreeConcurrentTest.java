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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeConcurrentTest extends AbstractRTreeTest {

    private static final int PAGE_SIZE = 1024;
    private static final int NUM_PAGES = 10000;
    private static final int MAX_OPEN_FILES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksStageletContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    @Test
    public void test01() throws Exception {

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare keys
        int keyFieldCount = 4;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = DoubleBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = cmps[0];
        cmps[2] = cmps[0];
        cmps[3] = cmps[0];

        // declare leaf-frame-tuple fields
        int fieldCount = 5;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(8);
        typeTraits[1] = new TypeTrait(8);
        typeTraits[2] = new TypeTrait(8);
        typeTraits[3] = new TypeTrait(8);
        typeTraits[4] = new TypeTrait(4);

        MultiComparator cmp = new MultiComparator(typeTraits, cmps);

        RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(typeTraits);

        ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(tupleWriterFactory,
                keyFieldCount);
        ITreeIndexFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IRTreeFrame interiorFrame = (IRTreeFrame) interiorFrameFactory.createFrame();
        IRTreeFrame leafFrame = (IRTreeFrame) leafFrameFactory.createFrame();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        RTree rtree = new RTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmp);
        rtree.create(fileId, leafFrame, metaFrame);
        rtree.open(fileId);

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        RTreeWriterTest writer1 = new RTreeWriterTest(rtree, ctx, "w1");
        writer1.start();

        RTreeWriterTest writer2 = new RTreeWriterTest(rtree, ctx, "w2");
        writer2.start();

        RTreeWriterTest writer3 = new RTreeWriterTest(rtree, ctx, "w3");
        writer3.start();

        RTreeWriterTest writer4 = new RTreeWriterTest(rtree, ctx, "w4");
        writer4.start();

        RTreeWriterTest writer5 = new RTreeWriterTest(rtree, ctx, "w5");
        writer5.start();

        RTreeWriterTest writer6 = new RTreeWriterTest(rtree, ctx, "w6");
        writer6.start();

        RTreeWriterTest writer7 = new RTreeWriterTest(rtree, ctx, "w7");
        writer7.start();

        RTreeWriterTest writer8 = new RTreeWriterTest(rtree, ctx, "w8");
        writer8.start();

        RTreeWriterTest writer9 = new RTreeWriterTest(rtree, ctx, "w9");
        writer9.start();

        RTreeWriterTest writer10 = new RTreeWriterTest(rtree, ctx, "w10");
        writer10.start();

        String stats = rtree.printStats();
        print(stats);

        System.out.println();

        stats = rtree.printStats();
        print(stats);

        System.out.println();
        rtree.printTree(leafFrame, interiorFrame, recDescSers);
        System.out.println();

        // rtree.close();
        // bufferCache.closeFile(fileId);
        // bufferCache.close();

    }
}
