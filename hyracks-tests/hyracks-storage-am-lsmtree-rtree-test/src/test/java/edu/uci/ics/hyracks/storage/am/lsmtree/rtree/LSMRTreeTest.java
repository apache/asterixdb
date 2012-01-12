package edu.uci.ics.hyracks.storage.am.lsmtree.rtree;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.logging.Level;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
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
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.impls.FreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.impls.InMemoryBufferCacheFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.impls.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls.LSMRTree;
import edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls.LSMTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls.RTreeFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class LSMRTreeTest extends AbstractLSMTreeTest {

    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10000;
    private static final int MAX_OPEN_FILES = 100;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    // INSERT-DELETE TEST
    @Test
    public void Test1() throws Exception {
        // in disk
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // in memory
        InMemoryBufferCacheFactory rtreeInMemBufferCacheFactory = new InMemoryBufferCacheFactory(PAGE_SIZE, NUM_PAGES);
        IBufferCache rtreeMemBufferCache = rtreeInMemBufferCacheFactory.createInMemoryBufferCache();

        // in memory
        InMemoryBufferCacheFactory btreeInMemBufferCacheFactory = new InMemoryBufferCacheFactory(PAGE_SIZE, NUM_PAGES);
        IBufferCache btreeMemBufferCache = btreeInMemBufferCacheFactory.createInMemoryBufferCache();

        // declare keys
        int keyFieldCount = 4;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        cmps[1] = cmps[0];
        cmps[2] = cmps[0];
        cmps[3] = cmps[0];

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

        MultiComparator cmp = new MultiComparator(cmps);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                cmps.length, DoublePointable.FACTORY);

        LSMTypeAwareTupleWriterFactory rtreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        LSMTypeAwareTupleWriterFactory btreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

        ITreeIndexFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);
        ITreeIndexFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories);

        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);

        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IFreePageManager rtreeMemFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);
        IFreePageManager btreeMemFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);

        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);

        RTreeFactory rTreeFactory = new RTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                btreeInteriorFrameFactory, btreeLeafFrameFactory);

        LSMRTree lsmrtree = new LSMRTree(rtreeMemBufferCache, btreeMemBufferCache, bufferCache, fieldCount, cmp,
                rtreeMemFreePageManager, btreeMemFreePageManager, rtreeInteriorFrameFactory, btreeInteriorFrameFactory,
                rtreeLeafFrameFactory, btreeLeafFrameFactory, rTreeFactory, bTreeFactory, (IFileMapManager) fmp);

        lsmrtree.create(fileId);
        lsmrtree.open(fileId);

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

        ITreeIndexAccessor lsmTreeAccessor = lsmrtree.createAccessor();

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
                // if (i % 1000 == 0) {
                LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " "
                        + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y));
                // }
            }

            try {
                lsmTreeAccessor.insert(tuple);
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
                //if (i % 1000 == 0) {
                    LOGGER.info("DELETING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " "
                            + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y));
                //}
            }

            try {
                lsmTreeAccessor.delete(tuple);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        lsmrtree.close();
        bufferCache.closeFile(fileId);
        rtreeMemBufferCache.close();
        btreeMemBufferCache.close();
    }

}
