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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.AbstractBTreeTest;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class BTreeTest extends AbstractBTreeTest {

    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int MAX_OPEN_FILES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    // FIXED-LENGTH KEY TEST
    // create a B-tree with one fixed-length "key" field and one fixed-length
    // "value" field
    // fill B-tree with random values using insertions (not bulk load)
    // perform ordered scan and range search
    @Test
    public void test01() throws Exception {

        LOGGER.info("FIXED-LENGTH KEY TEST");

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(4);
        typeTraits[1] = new TypeTrait(4);

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        MultiComparator cmp = new MultiComparator(cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);        
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        BTree btree = new BTree(bufferCache, fieldCount, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();

        LOGGER.info("INSERTING INTO TREE");

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        BTreeOpContext insertOpCtx = btree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);

        // 10000
        for (int i = 0; i < 10000; i++) {

            int f0 = rnd.nextInt() % 10000;
            int f1 = 5;

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            ArrayTupleReference t = new ArrayTupleReference();
            t.reset(tb.getFieldEndOffsets(), tb.getByteArray());
            
            if (i % 1000 == 0) {
                long end = System.currentTimeMillis();
                LOGGER.info("INSERTING " + i + " : " + f0 + " " + f1 + " " + (end - start));
            }

            try {
                btree.insert(t, insertOpCtx);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // btree.printTree(leafFrame, interiorFrame);

        int maxPage = btree.getFreePageManager().getMaxPage(metaFrame);
        LOGGER.info("MAXPAGE: " + maxPage);

        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("DURATION: " + duration);

        // ordered scan

        LOGGER.info("ORDERED SCAN:");
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        BTreeOpContext searchOpCtx = btree.createOpContext(IndexOp.SEARCH, leafFrame, interiorFrame, null);
        btree.search(scanCursor, nullPred, searchOpCtx);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // disk-order scan
        LOGGER.info("DISK-ORDER SCAN:");
        TreeDiskOrderScanCursor diskOrderCursor = new TreeDiskOrderScanCursor(leafFrame);
        BTreeOpContext diskOrderScanOpCtx = btree.createOpContext(IndexOp.DISKORDERSCAN, leafFrame, null, null);
        btree.diskOrderScan(diskOrderCursor, leafFrame, metaFrame, diskOrderScanOpCtx);
        try {
            while (diskOrderCursor.hasNext()) {
                diskOrderCursor.next();
                ITupleReference frameTuple = diskOrderCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            diskOrderCursor.close();
        }

        // range search in [-1000, 1000]
        LOGGER.info("RANGE SEARCH:");

        ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);

        // build low and high keys
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
        DataOutput kdos = ktb.getDataOutput();

        ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
        IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx.getFrameSize(), keyDesc);
        keyAccessor.reset(frame);

        appender.reset(frame, true);

        // build and append low key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(-1000, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // build and append high key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(1000, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // create tuplereferences for search keys
        FrameTupleReference lowKey = new FrameTupleReference();
        lowKey.reset(keyAccessor, 0);

        FrameTupleReference highKey = new FrameTupleReference();
        highKey.reset(keyAccessor, 1);

        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps);

        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, searchCmp, searchCmp);
        btree.search(rangeCursor, rangePred, searchOpCtx);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
    }

    // COMPOSITE KEY TEST (NON-UNIQUE B-TREE)
    // create a B-tree with one two fixed-length "key" fields and one
    // fixed-length "value" field
    // fill B-tree with random values using insertions (not bulk load)
    // perform ordered scan and range search
    @Test
    public void test02() throws Exception {

        LOGGER.info("COMPOSITE KEY TEST");

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 3;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(4);
        typeTraits[1] = new TypeTrait(4);
        typeTraits[2] = new TypeTrait(4);

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        MultiComparator cmp = new MultiComparator(cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        //ITreeIndexFrameFactory leafFrameFactory = new BTreeFieldPrefixNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        BTree btree = new BTree(bufferCache, fieldCount, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();

        LOGGER.info("INSERTING INTO TREE");

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        BTreeOpContext insertOpCtx = btree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);
        
        // Magic test number: 3029. 6398. 4875.
        for (int i = 0; i < 10000; i++) {
            int f0 = rnd.nextInt() % 2000;
            int f1 = rnd.nextInt() % 1000;
            int f2 = 5;            
            
            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
            tb.addFieldEndOffset();
            
            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            if (i % 1000 == 0) {
                LOGGER.info("INSERTING " + i + " : " + f0 + " " + f1);
            }
            
            try {
                btree.insert(tuple, insertOpCtx);
            } catch (Exception e) {
            }
            
            //ISerializerDeserializer[] keySerdes = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
            //btree.printTree(leafFrame, interiorFrame, keySerdes);
            //System.out.println("---------------------------------");
        }

        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("DURATION: " + duration);

        // try a simple index scan
        LOGGER.info("ORDERED SCAN:");
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        BTreeOpContext searchOpCtx = btree.createOpContext(IndexOp.SEARCH, leafFrame, interiorFrame, null);
        btree.search(scanCursor, nullPred, searchOpCtx);

        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // range search in [(-3),(3)]
        LOGGER.info("RANGE SEARCH:");
        ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);

        // build low and high keys
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
        DataOutput kdos = ktb.getDataOutput();

        ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
        IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx.getFrameSize(), keyDesc);
        keyAccessor.reset(frame);

        appender.reset(frame, true);

        // build and append low key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(-3, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // build and append high key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(3, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // create tuplereferences for search keys
        FrameTupleReference lowKey = new FrameTupleReference();
        lowKey.reset(keyAccessor, 0);

        FrameTupleReference highKey = new FrameTupleReference();
        highKey.reset(keyAccessor, 1);

        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps); // use
        // only
        // a
        // single
        // comparator
        // for
        // searching

        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, searchCmp, searchCmp);
        btree.search(rangeCursor, rangePred, searchOpCtx);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }


        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
    }

    // VARIABLE-LENGTH TEST
    // create a B-tree with one variable-length "key" field and one
    // variable-length "value" field
    // fill B-tree with random values using insertions (not bulk load)
    // perform ordered scan and range search
    @Test
    public void test03() throws Exception {

        LOGGER.info("VARIABLE-LENGTH KEY TEST");

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        MultiComparator cmp = new MultiComparator(cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        BTree btree = new BTree(bufferCache, fieldCount, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        BTreeOpContext insertOpCtx = btree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);
        int maxLength = 10; // max string length to be generated
        for (int i = 0; i < 10000; i++) {

            String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);

            tb.reset();
            UTF8StringSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            UTF8StringSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            if (i % 1000 == 0) {
                LOGGER.info("INSERTING " + i);
            }

            try {
                btree.insert(tuple, insertOpCtx);
            } catch (Exception e) {
            }
        }
        // btree.printTree();

        LOGGER.info("DONE INSERTING");

        // ordered scan
        LOGGER.info("ORDERED SCAN:");
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        BTreeOpContext searchOpCtx = btree.createOpContext(IndexOp.SEARCH, leafFrame, interiorFrame, null);
        btree.search(scanCursor, nullPred, searchOpCtx);

        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // range search in ["cbf", cc7"]
        LOGGER.info("RANGE SEARCH:");

        ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);

        // build low and high keys
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
        DataOutput kdos = ktb.getDataOutput();

        ISerializerDeserializer[] keyDescSers = { UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
        IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx.getFrameSize(), keyDesc);
        keyAccessor.reset(frame);

        appender.reset(frame, true);

        // build and append low key
        ktb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize("cbf", kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // build and append high key
        ktb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize("cc7", kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // create tuplereferences for search keys
        FrameTupleReference lowKey = new FrameTupleReference();
        lowKey.reset(keyAccessor, 0);

        FrameTupleReference highKey = new FrameTupleReference();
        highKey.reset(keyAccessor, 1);

        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps);

        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, searchCmp, searchCmp);
        btree.search(rangeCursor, rangePred, searchOpCtx);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
    }

    // DELETION TEST
    // create a B-tree with one variable-length "key" field and one
    // variable-length "value" field
    // fill B-tree with random values using insertions, then delete entries
    // one-by-one
    // repeat procedure a few times on same B-tree
    @Test
    public void test04() throws Exception {

        LOGGER.info("DELETION TEST");

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        MultiComparator cmp = new MultiComparator(cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        BTree btree = new BTree(bufferCache, fieldCount, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        BTreeOpContext insertOpCtx = btree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);
        BTreeOpContext deleteOpCtx = btree.createOpContext(IndexOp.DELETE, leafFrame, interiorFrame, metaFrame);

        int runs = 3;
        for (int run = 0; run < runs; run++) {

            LOGGER.info("DELETION TEST RUN: " + (run + 1) + "/" + runs);

            LOGGER.info("INSERTING INTO BTREE");
            int maxLength = 10;
            int ins = 10000;
            String[] f0s = new String[ins];
            String[] f1s = new String[ins];
            int insDone = 0;
            int[] insDoneCmp = new int[ins];
            for (int i = 0; i < ins; i++) {
                String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);

                f0s[i] = f0;
                f1s[i] = f1;

                tb.reset();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(f0, dos);
                tb.addFieldEndOffset();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(f1, dos);
                tb.addFieldEndOffset();

                appender.reset(frame, true);
                appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

                tuple.reset(accessor, 0);

                if (i % 1000 == 0) {
                    LOGGER.info("INSERTING " + i);
                }

                try {
                    btree.insert(tuple, insertOpCtx);
                    insDone++;
                } catch (TreeIndexException e) {
                    // e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                insDoneCmp[i] = insDone;
            }
            // btree.printTree();
            // btree.printStats();

            LOGGER.info("DELETING FROM BTREE");
            int delDone = 0;
            for (int i = 0; i < ins; i++) {

                tb.reset();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(f0s[i], dos);
                tb.addFieldEndOffset();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(f1s[i], dos);
                tb.addFieldEndOffset();

                appender.reset(frame, true);
                appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

                tuple.reset(accessor, 0);

                if (i % 1000 == 0) {
                    LOGGER.info("DELETING " + i);
                }

                try {
                    btree.delete(tuple, deleteOpCtx);
                    delDone++;
                } catch (TreeIndexException e) {
                    // e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (insDoneCmp[i] != delDone) {
                    LOGGER.info("INCONSISTENT STATE, ERROR IN DELETION TEST");
                    LOGGER.info("INSDONECMP: " + insDoneCmp[i] + " " + delDone);
                    break;
                }
            }
            // btree.printTree(leafFrame, interiorFrame);

            if (insDone != delDone) {
                LOGGER.info("ERROR! INSDONE: " + insDone + " DELDONE: " + delDone);
                break;
            }
        }

        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
    }

    
    private void orderedScan(BTree btree, IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame, ISerializerDeserializer[] recDescSers) throws Exception {
        // try a simple index scan
        LOGGER.info("ORDERED SCAN:");
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        BTreeOpContext searchOpCtx = btree.createOpContext(IndexOp.SEARCH, leafFrame, interiorFrame, null);
        btree.search(scanCursor, nullPred, searchOpCtx);
        StringBuilder scanResults = new StringBuilder();
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                scanResults.append("\n" + rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }
        LOGGER.info(scanResults.toString());
    }
    
    // Assuming exactly two BTree fields.
    private void compareActualAndExpected(ITreeIndexCursor actualCursor, Map<String, String> expectedValues, ISerializerDeserializer[] fieldSerdes) throws Exception {
        while (actualCursor.hasNext()) {
            actualCursor.next();
            ITupleReference tuple = actualCursor.getTuple();
            String f0 = (String) deserializeField(tuple, 0, fieldSerdes[0]);
            String f1 = (String) deserializeField(tuple, 1, fieldSerdes[1]);
            String expected = expectedValues.get(f0);
            if (!expected.equals(f1)) {
                throw new Exception("FAILED: " + f0 + " " + f1 + " " + expected);
            }
        }
    }
    
    private Object deserializeField(ITupleReference tuple, int fIdx, ISerializerDeserializer serde) throws HyracksDataException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(tuple.getFieldData(fIdx), tuple.getFieldStart(fIdx), tuple.getFieldLength(fIdx)));
        return serde.deserialize(in);
    }
    
    // UPDATE TEST
    // create a B-tree with one variable-length "key" field and one
    // variable-length "value" field
    // fill B-tree with random values using insertions, then update entries
    // one-by-one
    // repeat procedure a few times on same B-tree
    @Test
    public void test05() throws Exception {

        LOGGER.info("DELETION TEST");

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        MultiComparator cmp = new MultiComparator(cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        BTree btree = new BTree(bufferCache, fieldCount, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(fieldSerdes);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        BTreeOpContext insertOpCtx = btree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);
        BTreeOpContext updateOpCtx = btree.createOpContext(IndexOp.UPDATE, leafFrame, interiorFrame, metaFrame);

        Map<String, String> expectedValues = new HashMap<String, String>();
        
        LOGGER.info("INSERTING INTO BTREE");
        int maxLength = 10;
        int ins = 10000;
        // Only remember the keys.
        String[] f0s = new String[ins];
        for (int i = 0; i < ins; i++) {
            String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);

            f0s[i] = f0;

            tb.reset();
            UTF8StringSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            UTF8StringSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            ArrayTupleReference t = new ArrayTupleReference();
            t.reset(tb.getFieldEndOffsets(), tb.getByteArray());
            
            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            if (i % 1000 == 0) {
                LOGGER.info("INSERTING " + i);
            }
            try {
                btree.insert(t, insertOpCtx);
                expectedValues.put(f0, f1);
            } catch (TreeIndexException e) {
                // e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        ITreeIndexCursor insertCheckCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        BTreeOpContext searchOpCtx = btree.createOpContext(IndexOp.SEARCH, leafFrame, interiorFrame, null);
        btree.search(insertCheckCursor, nullPred, searchOpCtx);
        try {
            compareActualAndExpected(insertCheckCursor, expectedValues, fieldSerdes);
        } finally {
            insertCheckCursor.close();
        }
        
        int runs = 3;
        for (int run = 0; run < runs; run++) {

            LOGGER.info("UPDATE TEST RUN: " + (run + 1) + "/" + runs);

            LOGGER.info("UPDATING BTREE");
            for (int i = 0; i < ins; i++) {
                // Generate a new random value for f1.
                String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                
                tb.reset();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(f0s[i], dos);
                tb.addFieldEndOffset();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(f1, dos);
                tb.addFieldEndOffset();

                appender.reset(frame, true);
                appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

                tuple.reset(accessor, 0);

                if (i % 1000 == 0) {
                    LOGGER.info("UPDATING " + i);
                }

                ArrayTupleReference t = new ArrayTupleReference();
                t.reset(tb.getFieldEndOffsets(), tb.getByteArray());
                
                try {
                    btree.update(t, updateOpCtx);
                    expectedValues.put(f0s[i], f1);
                } catch (TreeIndexException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            
            ITreeIndexCursor updateCheckCursor = new BTreeRangeSearchCursor(leafFrame);
            btree.search(updateCheckCursor, nullPred, searchOpCtx);
            try {
                compareActualAndExpected(updateCheckCursor, expectedValues, fieldSerdes);
            } finally {
                updateCheckCursor.close();
            }
        }

        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
    }
    
    // BULK LOAD TEST
    // insert 100,000 records in bulk
    // B-tree has a composite key to "simulate" non-unique index creation
    // do range search
    @Test
    public void test06() throws Exception {

        LOGGER.info("BULK LOAD TEST");

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 3;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(4);
        typeTraits[1] = new TypeTrait(4);
        typeTraits[2] = new TypeTrait(4);

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        MultiComparator cmp = new MultiComparator(cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
        ITreeIndexFrame interiorFrame = interiorFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        BTree btree = new BTree(bufferCache, fieldCount, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        IIndexBulkLoadContext bulkLoadCtx = btree.beginBulkLoad(0.7f, leafFrame, interiorFrame, metaFrame);

        // generate sorted records
        int ins = 100000;
        LOGGER.info("BULK LOADING " + ins + " RECORDS");
        long start = System.currentTimeMillis();
        for (int i = 0; i < ins; i++) {

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(i, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(i, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(5, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            btree.bulkLoadAddTuple(tuple, bulkLoadCtx);
        }

        btree.endBulkLoad(bulkLoadCtx);

        // btree.printTree(leafFrame, interiorFrame);

        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("DURATION: " + duration);

        // range search
        LOGGER.info("RANGE SEARCH:");
        ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) leafFrame);

        // build low and high keys
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(1);
        DataOutput kdos = ktb.getDataOutput();

        ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
        IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx.getFrameSize(), keyDesc);
        keyAccessor.reset(frame);

        appender.reset(frame, true);

        // build and append low key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(44444, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // build and append high key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(44500, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // create tuplereferences for search keys
        FrameTupleReference lowKey = new FrameTupleReference();
        lowKey.reset(keyAccessor, 0);

        FrameTupleReference highKey = new FrameTupleReference();
        highKey.reset(keyAccessor, 1);

        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps);

        // TODO: check when searching backwards
        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, searchCmp, searchCmp);
        BTreeOpContext searchOpCtx = btree.createOpContext(IndexOp.SEARCH, leafFrame, interiorFrame, null);
        btree.search(rangeCursor, rangePred, searchOpCtx);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
    }

    // TIME-INTERVAL INTERSECTION DEMO FOR EVENT PEOPLE
    // demo for Arjun to show easy support of intersection queries on
    // time-intervals
    //@Test
    public void test07() throws Exception {

        LOGGER.info("TIME-INTERVAL INTERSECTION DEMO");

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 3;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(4);
        typeTraits[1] = new TypeTrait(4);
        typeTraits[2] = new TypeTrait(4);

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator cmp = new MultiComparator(cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        BTree btree = new BTree(bufferCache, fieldCount, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        long start = System.currentTimeMillis();

        int intervalCount = 10;
        int[][] intervals = new int[intervalCount][2];

        intervals[0][0] = 10;
        intervals[0][1] = 20;

        intervals[1][0] = 11;
        intervals[1][1] = 20;

        intervals[2][0] = 12;
        intervals[2][1] = 20;

        intervals[3][0] = 13;
        intervals[3][1] = 20;

        intervals[4][0] = 14;
        intervals[4][1] = 20;

        intervals[5][0] = 20;
        intervals[5][1] = 30;

        intervals[6][0] = 20;
        intervals[6][1] = 31;

        intervals[7][0] = 20;
        intervals[7][1] = 32;

        intervals[8][0] = 20;
        intervals[8][1] = 33;

        intervals[9][0] = 20;
        intervals[9][1] = 35;

        BTreeOpContext insertOpCtx = btree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);

        // int exceptionCount = 0;
        for (int i = 0; i < intervalCount; i++) {
            int f0 = intervals[i][0];
            int f1 = intervals[i][1];
            int f2 = rnd.nextInt() % 100;

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            LOGGER.info("INSERTING " + i);

            try {
                btree.insert(tuple, insertOpCtx);
            } catch (Exception e) {
            }
        }
        // btree.printTree(leafFrame, interiorFrame);
        // btree.printStats();

        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("DURATION: " + duration);

        // try a simple index scan

        LOGGER.info("ORDERED SCAN:");
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        BTreeOpContext searchOpCtx = btree.createOpContext(IndexOp.SEARCH, leafFrame, interiorFrame, null);
        btree.search(scanCursor, nullPred, searchOpCtx);

        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                // TODO: fix me.
                //print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // try a range search
        LOGGER.info("RANGE SEARCH:");
        ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);

        // build low and high keys
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
        DataOutput kdos = ktb.getDataOutput();

        ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
        IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx.getFrameSize(), keyDesc);
        keyAccessor.reset(frame);

        appender.reset(frame, true);

        // build and append low key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(12, kdos);
        ktb.addFieldEndOffset();
        IntegerSerializerDeserializer.INSTANCE.serialize(12, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // build and append high key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(19, kdos);
        ktb.addFieldEndOffset();
        IntegerSerializerDeserializer.INSTANCE.serialize(19, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // create tuplereferences for search keys
        FrameTupleReference lowKey = new FrameTupleReference();
        lowKey.reset(keyAccessor, 0);

        FrameTupleReference highKey = new FrameTupleReference();
        highKey.reset(keyAccessor, 1);

        IBinaryComparator[] searchCmps = new IBinaryComparator[2];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        searchCmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps);

        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, searchCmp, searchCmp);
        btree.search(rangeCursor, rangePred, searchOpCtx);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
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