package edu.uci.ics.hyracks.storage.am.rtree;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.utility.TreeIndexStats;
import edu.uci.ics.hyracks.storage.am.common.utility.TreeIndexStatsGatherer;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeTest extends AbstractRTreeTest {

    private static final int PAGE_SIZE = 8192;
    private static final int NUM_PAGES = 20;
    private static final int MAX_OPEN_FILES = 20;
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

        // declare tuple fields
        int fieldCount = 7;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(8);
        typeTraits[1] = new TypeTrait(8);
        typeTraits[2] = new TypeTrait(8);
        typeTraits[3] = new TypeTrait(8);
        typeTraits[4] = new TypeTrait(8);
        typeTraits[5] = new TypeTrait(4);
        typeTraits[6] = new TypeTrait(8);

        MultiComparator cmp = new MultiComparator(typeTraits, cmps);

        RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(typeTraits);

        ITreeIndexFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexFrameFactory leafFrameFactory = new NSMLeafFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IRTreeFrame interiorFrame = (IRTreeFrame) interiorFrameFactory.createFrame();
        IRTreeFrame leafFrame = (IRTreeFrame) leafFrameFactory.createFrame();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        RTree rtree = new RTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmp);
        rtree.create(fileId, leafFrame, metaFrame);
        rtree.open(fileId);

        ByteBuffer hyracksFrame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(hyracksFrame);
        FrameTupleReference tuple = new FrameTupleReference();

        RTreeOpContext insertOpCtx = rtree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);

        Random rnd = new Random();
        rnd.setSeed(50);

        Random rnd2 = new Random();
        rnd2.setSeed(50);
        for (int i = 0; i < 10000; i++) {

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

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            print("INSERTING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
                    + " " + Math.max(p1y, p2y) + "\n");

            try {
                rtree.insert(tuple, insertOpCtx);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // rtree.printTree(leafFrame, interiorFrame, recDescSers);
        // System.out.println();

        String rtreeStats = rtree.printStats();
        print(rtreeStats);

        // disk-order scan
        print("DISK-ORDER SCAN:\n");
        RTreeDiskOrderScanCursor diskOrderCursor = new RTreeDiskOrderScanCursor(leafFrame);
        RTreeOpContext diskOrderScanOpCtx = rtree.createOpContext(IndexOp.DISKORDERSCAN, leafFrame, null, null);
        rtree.diskOrderScan(diskOrderCursor, leafFrame, metaFrame, diskOrderScanOpCtx);
        try {
            while (diskOrderCursor.hasNext()) {
                diskOrderCursor.next();
                ITupleReference frameTuple = diskOrderCursor.getTuple();
                String rec = cmp.printTuple(frameTuple, recDescSers);
                print(rec + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            diskOrderCursor.close();
        }

        TreeIndexStatsGatherer statsGatherer = new TreeIndexStatsGatherer(bufferCache, freePageManager, fileId,
                rtree.getRootPageId());
        TreeIndexStats stats = statsGatherer.gatherStats(leafFrame, interiorFrame, metaFrame);
        String string = stats.toString();
        System.out.println(string);

        rtree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();

    }
}
