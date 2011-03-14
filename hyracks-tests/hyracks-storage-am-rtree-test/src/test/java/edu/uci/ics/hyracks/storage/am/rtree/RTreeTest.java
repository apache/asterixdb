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
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.TreeIndexOp;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.NSMRTreeFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeTest extends AbstractRTreeTest {

    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksStageletContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    @Test
    public void test01() throws Exception {

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare interior-frame-tuple fields
        int interiorTieldCount = 5;
        ITypeTrait[] interiorTypeTraits = new ITypeTrait[interiorTieldCount];
        interiorTypeTraits[0] = new TypeTrait(8);
        interiorTypeTraits[1] = new TypeTrait(8);
        interiorTypeTraits[2] = new TypeTrait(8);
        interiorTypeTraits[3] = new TypeTrait(8);
        interiorTypeTraits[4] = new TypeTrait(4);

        // declare keys
        int keyFieldCount = 4;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = DoubleBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = cmps[0];
        cmps[2] = cmps[0];
        cmps[3] = cmps[0];

        // declare leaf-frame-tuple fields
        int leafFieldCount = 5;
        ITypeTrait[] leafTypeTraits = new ITypeTrait[leafFieldCount];
        leafTypeTraits[0] = new TypeTrait(8);
        leafTypeTraits[1] = new TypeTrait(8);
        leafTypeTraits[2] = new TypeTrait(8);
        leafTypeTraits[3] = new TypeTrait(8);
        leafTypeTraits[4] = new TypeTrait(4);

        MultiComparator interiorCmp = new MultiComparator(interiorTypeTraits, cmps);
        MultiComparator leafCmp = new MultiComparator(leafTypeTraits, cmps);

        RTreeTypeAwareTupleWriterFactory interiorTupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
                interiorTypeTraits);
        RTreeTypeAwareTupleWriterFactory leafTupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(leafTypeTraits);

        IRTreeFrameFactory interiorFrameFactory = new NSMRTreeFrameFactory(interiorTupleWriterFactory);
        IRTreeFrameFactory leafFrameFactory = new NSMRTreeFrameFactory(leafTupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.getFrame();

        IRTreeFrame interiorFrame = interiorFrameFactory.getFrame();
        IRTreeFrame leafFrame = leafFrameFactory.getFrame();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0);

        RTree rtree = new RTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, interiorCmp,
                leafCmp);
        rtree.create(fileId, leafFrame, metaFrame);
        rtree.open(fileId);

        ByteBuffer hyracksFrame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(leafCmp.getFieldCount());
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(hyracksFrame);
        FrameTupleReference tuple = new FrameTupleReference();

        RTreeOpContext insertOpCtx = rtree.createOpContext(TreeIndexOp.TI_INSERT, interiorFrame, leafFrame, metaFrame);

        Random rnd = new Random();
        rnd.setSeed(50);

        for (int i = 0; i < 10000; i++) {

            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();

            int pk = rnd.nextInt();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x), dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y), dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk, dos);
            tb.addFieldEndOffset();

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            // if (i % 1000 == 0) {
            long end = System.currentTimeMillis();
            print("INSERTING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
                    + " " + Math.max(p1y, p2y) + "\n");
            // }

            try {
                rtree.insert(tuple, insertOpCtx);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        rtree.printTree(leafFrame, interiorFrame, recDescSers);
        System.out.println();

        String stats = rtree.printStats();
        print(stats);

        rtree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();

    }
}
