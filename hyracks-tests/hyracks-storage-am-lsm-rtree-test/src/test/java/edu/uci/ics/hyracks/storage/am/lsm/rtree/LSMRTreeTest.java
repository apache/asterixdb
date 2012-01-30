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

package edu.uci.ics.hyracks.storage.am.lsm.rtree;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.logging.Level;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileNameManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeFileNameManager;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.RTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.tuples.LSMTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

public class LSMRTreeTest extends AbstractLSMRTreeTest {

    // create an LSMRTree of two dimensions
    // fill the tree with random values using insertions
    @Test
    public void test01() throws Exception {

        // declare r-tree keys
        int rtreeKeyFieldCount = 4;
        IBinaryComparator[] rtreeCmps = new IBinaryComparator[rtreeKeyFieldCount];
        rtreeCmps[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        rtreeCmps[1] = rtreeCmps[0];
        rtreeCmps[2] = rtreeCmps[0];
        rtreeCmps[3] = rtreeCmps[0];

        // declare b-tree keys
        int btreeKeyFieldCount = 7;
        IBinaryComparator[] btreeCmps = new IBinaryComparator[btreeKeyFieldCount];
        btreeCmps[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        btreeCmps[1] = btreeCmps[0];
        btreeCmps[2] = btreeCmps[0];
        btreeCmps[3] = btreeCmps[0];
        btreeCmps[4] = btreeCmps[0];
        btreeCmps[5] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        btreeCmps[6] = btreeCmps[0];

        // declare tuple fields
        int fieldCount = 7;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = DoublePointable.TYPE_TRAITS;
        typeTraits[1] = DoublePointable.TYPE_TRAITS;
        typeTraits[2] = DoublePointable.TYPE_TRAITS;
        typeTraits[3] = DoublePointable.TYPE_TRAITS;
        typeTraits[4] = DoublePointable.TYPE_TRAITS;
        typeTraits[5] = IntegerPointable.TYPE_TRAITS;
        typeTraits[6] = DoublePointable.TYPE_TRAITS;

        MultiComparator rtreeCmp = new MultiComparator(rtreeCmps);
        MultiComparator btreeCmp = new MultiComparator(btreeCmps);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmps.length, DoublePointable.FACTORY);

        LSMTypeAwareTupleWriterFactory rtreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        LSMTypeAwareTupleWriterFactory btreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

        ITreeIndexFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);
        ITreeIndexFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);

        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);

        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        InMemoryFreePageManager memFreePageManager = new LSMRTreeInMemoryFreePageManager(1000, metaFrameFactory);

        LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);

        RTreeFactory diskRTreeFactory = new RTreeFactory(diskBufferCache, freePageManagerFactory, rtreeCmp, fieldCount,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory);
        BTreeFactory diskBTreeFactory = new BTreeFactory(diskBufferCache, freePageManagerFactory, btreeCmp, fieldCount,
                btreeInteriorFrameFactory, btreeLeafFrameFactory);

        ILSMFileNameManager fileNameManager = new LSMTreeFileNameManager(onDiskDir);
        LSMRTree lsmRTree = new LSMRTree(memBufferCache, memFreePageManager, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory, btreeInteriorFrameFactory, btreeLeafFrameFactory, fileNameManager,
                diskRTreeFactory, diskBTreeFactory, diskFileMapProvider, fieldCount, rtreeCmp, btreeCmp);

        lsmRTree.create(getFileId());
        lsmRTree.open(getFileId());

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        ITreeIndexAccessor indexAccessor = lsmRTree.createAccessor();

        Random rnd = new Random();
        rnd.setSeed(50);

        Random rnd2 = new Random();
        rnd2.setSeed(50);
        for (int i = 0; i < 5000; i++) {

            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();

            double pk1 = rnd2.nextDouble();
            int pk2 = rnd2.nextInt();
            double pk3 = rnd2.nextDouble();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " "
                            + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y));
                }
            }

            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        lsmRTree.close();
        memBufferCache.close();

    }

    // create an LSMRTree of two dimensions
    // fill the tree with random values using insertions
    // and then delete all the tuples which result of an empty LSMRTree
    @Test
    public void test02() throws Exception {

        // declare r-tree keys
        int rtreeKeyFieldCount = 4;
        IBinaryComparator[] rtreeCmps = new IBinaryComparator[rtreeKeyFieldCount];
        rtreeCmps[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        rtreeCmps[1] = rtreeCmps[0];
        rtreeCmps[2] = rtreeCmps[0];
        rtreeCmps[3] = rtreeCmps[0];

        // declare b-tree keys
        int btreeKeyFieldCount = 7;
        IBinaryComparator[] btreeCmps = new IBinaryComparator[btreeKeyFieldCount];
        btreeCmps[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        btreeCmps[1] = btreeCmps[0];
        btreeCmps[2] = btreeCmps[0];
        btreeCmps[3] = btreeCmps[0];
        btreeCmps[4] = btreeCmps[0];
        btreeCmps[5] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        btreeCmps[6] = btreeCmps[0];

        // declare tuple fields
        int fieldCount = 7;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = DoublePointable.TYPE_TRAITS;
        typeTraits[1] = DoublePointable.TYPE_TRAITS;
        typeTraits[2] = DoublePointable.TYPE_TRAITS;
        typeTraits[3] = DoublePointable.TYPE_TRAITS;
        typeTraits[4] = DoublePointable.TYPE_TRAITS;
        typeTraits[5] = IntegerPointable.TYPE_TRAITS;
        typeTraits[6] = DoublePointable.TYPE_TRAITS;

        MultiComparator rtreeCmp = new MultiComparator(rtreeCmps);
        MultiComparator btreeCmp = new MultiComparator(btreeCmps);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmps.length, DoublePointable.FACTORY);

        LSMTypeAwareTupleWriterFactory rtreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        LSMTypeAwareTupleWriterFactory btreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

        ITreeIndexFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);
        ITreeIndexFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);

        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);

        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        InMemoryFreePageManager memFreePageManager = new LSMRTreeInMemoryFreePageManager(1000, metaFrameFactory);

        LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);

        RTreeFactory diskRTreeFactory = new RTreeFactory(diskBufferCache, freePageManagerFactory, rtreeCmp, fieldCount,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory);
        BTreeFactory diskBTreeFactory = new BTreeFactory(diskBufferCache, freePageManagerFactory, btreeCmp, fieldCount,
                btreeInteriorFrameFactory, btreeLeafFrameFactory);

        ILSMFileNameManager fileNameManager = new LSMTreeFileNameManager(onDiskDir);
        LSMRTree lsmRTree = new LSMRTree(memBufferCache, memFreePageManager, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory, btreeInteriorFrameFactory, btreeLeafFrameFactory, fileNameManager,
                diskRTreeFactory, diskBTreeFactory, diskFileMapProvider, fieldCount, rtreeCmp, btreeCmp);

        lsmRTree.create(getFileId());
        lsmRTree.open(getFileId());

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        ITreeIndexAccessor indexAccessor = lsmRTree.createAccessor();

        Random rnd = new Random();
        rnd.setSeed(50);

        for (int i = 0; i < 5000; i++) {

            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();

            double pk1 = rnd.nextDouble();
            int pk2 = rnd.nextInt();
            double pk3 = rnd.nextDouble();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " "
                            + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y));
                }
            }

            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        rnd.setSeed(50);
        for (int i = 0; i < 5000; i++) {

            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();

            double pk1 = rnd.nextDouble();
            int pk2 = rnd.nextInt();
            double pk3 = rnd.nextDouble();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("DELETING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " "
                            + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y));
                }
            }

            try {
                indexAccessor.delete(tuple);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        lsmRTree.close();
        memBufferCache.close();

    }

    // create an LSMRTree of three dimensions
    // fill the tree with random values using insertions
    @Test
    public void test03() throws Exception {

        // declare r-tree keys
        int rtreeKeyFieldCount = 6;
        IBinaryComparator[] rtreeCmps = new IBinaryComparator[rtreeKeyFieldCount];
        rtreeCmps[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        rtreeCmps[1] = rtreeCmps[0];
        rtreeCmps[2] = rtreeCmps[0];
        rtreeCmps[3] = rtreeCmps[0];
        rtreeCmps[4] = rtreeCmps[0];
        rtreeCmps[5] = rtreeCmps[0];

        // declare b-tree keys
        int btreeKeyFieldCount = 9;
        IBinaryComparator[] btreeCmps = new IBinaryComparator[btreeKeyFieldCount];
        btreeCmps[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        btreeCmps[1] = btreeCmps[0];
        btreeCmps[2] = btreeCmps[0];
        btreeCmps[3] = btreeCmps[0];
        btreeCmps[4] = btreeCmps[0];
        btreeCmps[5] = btreeCmps[0];
        btreeCmps[6] = btreeCmps[0];
        btreeCmps[7] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        btreeCmps[8] = btreeCmps[0];

        // declare tuple fields
        int fieldCount = 9;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = DoublePointable.TYPE_TRAITS;
        typeTraits[1] = DoublePointable.TYPE_TRAITS;
        typeTraits[2] = DoublePointable.TYPE_TRAITS;
        typeTraits[3] = DoublePointable.TYPE_TRAITS;
        typeTraits[4] = DoublePointable.TYPE_TRAITS;
        typeTraits[5] = DoublePointable.TYPE_TRAITS;
        typeTraits[6] = DoublePointable.TYPE_TRAITS;
        typeTraits[7] = IntegerPointable.TYPE_TRAITS;
        typeTraits[8] = DoublePointable.TYPE_TRAITS;

        MultiComparator rtreeCmp = new MultiComparator(rtreeCmps);
        MultiComparator btreeCmp = new MultiComparator(btreeCmps);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmps.length, DoublePointable.FACTORY);

        LSMTypeAwareTupleWriterFactory rtreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        LSMTypeAwareTupleWriterFactory btreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

        ITreeIndexFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);
        ITreeIndexFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);

        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);

        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        InMemoryFreePageManager memFreePageManager = new LSMRTreeInMemoryFreePageManager(1000, metaFrameFactory);

        LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);

        RTreeFactory diskRTreeFactory = new RTreeFactory(diskBufferCache, freePageManagerFactory, rtreeCmp, fieldCount,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory);
        BTreeFactory diskBTreeFactory = new BTreeFactory(diskBufferCache, freePageManagerFactory, btreeCmp, fieldCount,
                btreeInteriorFrameFactory, btreeLeafFrameFactory);

        ILSMFileNameManager fileNameManager = new LSMTreeFileNameManager(onDiskDir);
        LSMRTree lsmRTree = new LSMRTree(memBufferCache, memFreePageManager, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory, btreeInteriorFrameFactory, btreeLeafFrameFactory, fileNameManager,
                diskRTreeFactory, diskBTreeFactory, diskFileMapProvider, fieldCount, rtreeCmp, btreeCmp);

        lsmRTree.create(getFileId());
        lsmRTree.open(getFileId());

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        ITreeIndexAccessor indexAccessor = lsmRTree.createAccessor();

        Random rnd = new Random();
        rnd.setSeed(50);

        for (int i = 0; i < 5000; i++) {

            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p1z = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();
            double p2z = rnd.nextDouble();

            double pk1 = rnd.nextDouble();
            int pk2 = rnd.nextInt();
            double pk3 = rnd.nextDouble();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1z, p2z), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1z, p2z), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " "
                            + Math.min(p1z, p2z) + " " + " " + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y) + " "
                            + Math.max(p1z, p2z));
                }
            }

            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        lsmRTree.close();
        memBufferCache.close();

    }

    // create an LSMRTree of two dimensions
    // fill the tree with random integer key values using insertions
    @Test
    public void test04() throws Exception {

        // declare r-tree keys
        int rtreeKeyFieldCount = 4;
        IBinaryComparator[] rtreeCmps = new IBinaryComparator[rtreeKeyFieldCount];
        rtreeCmps[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        rtreeCmps[1] = rtreeCmps[0];
        rtreeCmps[2] = rtreeCmps[0];
        rtreeCmps[3] = rtreeCmps[0];

        // declare b-tree keys
        int btreeKeyFieldCount = 7;
        IBinaryComparator[] btreeCmps = new IBinaryComparator[btreeKeyFieldCount];
        btreeCmps[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        btreeCmps[1] = btreeCmps[0];
        btreeCmps[2] = btreeCmps[0];
        btreeCmps[3] = btreeCmps[0];
        btreeCmps[4] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        btreeCmps[5] = btreeCmps[0];
        btreeCmps[6] = btreeCmps[4];

        // declare tuple fields
        int fieldCount = 7;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = IntegerPointable.TYPE_TRAITS;
        typeTraits[4] = DoublePointable.TYPE_TRAITS;
        typeTraits[5] = IntegerPointable.TYPE_TRAITS;
        typeTraits[6] = DoublePointable.TYPE_TRAITS;

        MultiComparator rtreeCmp = new MultiComparator(rtreeCmps);
        MultiComparator btreeCmp = new MultiComparator(btreeCmps);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmps.length, IntegerPointable.FACTORY);

        LSMTypeAwareTupleWriterFactory rtreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        LSMTypeAwareTupleWriterFactory btreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

        ITreeIndexFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);
        ITreeIndexFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);

        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);

        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        InMemoryFreePageManager memFreePageManager = new LSMRTreeInMemoryFreePageManager(1000, metaFrameFactory);

        LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);

        RTreeFactory diskRTreeFactory = new RTreeFactory(diskBufferCache, freePageManagerFactory, rtreeCmp, fieldCount,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory);
        BTreeFactory diskBTreeFactory = new BTreeFactory(diskBufferCache, freePageManagerFactory, btreeCmp, fieldCount,
                btreeInteriorFrameFactory, btreeLeafFrameFactory);

        ILSMFileNameManager fileNameManager = new LSMTreeFileNameManager(onDiskDir);
        LSMRTree lsmRTree = new LSMRTree(memBufferCache, memFreePageManager, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory, btreeInteriorFrameFactory, btreeLeafFrameFactory, fileNameManager,
                diskRTreeFactory, diskBTreeFactory, diskFileMapProvider, fieldCount, rtreeCmp, btreeCmp);

        lsmRTree.create(getFileId());
        lsmRTree.open(getFileId());

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        ITreeIndexAccessor indexAccessor = lsmRTree.createAccessor();

        Random rnd = new Random();
        rnd.setSeed(50);

        Random rnd2 = new Random();
        rnd2.setSeed(50);
        for (int i = 0; i < 5000; i++) {

            int p1x = rnd.nextInt();
            int p1y = rnd.nextInt();
            int p2x = rnd.nextInt();
            int p2y = rnd.nextInt();

            double pk1 = rnd2.nextDouble();
            int pk2 = rnd2.nextInt();
            double pk3 = rnd2.nextDouble();

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x), dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y), dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x), dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " "
                            + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y));
                }
            }

            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        lsmRTree.close();
        memBufferCache.close();

    }
}