package edu.uci.ics.hyracks.storage.am.rtree;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.Stack;

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
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.NSMRTreeFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeTest extends AbstractRTreeTest {

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

        // declare interior-frame-tuple fields
        int interiorFieldCount = 5;
        ITypeTrait[] interiorTypeTraits = new ITypeTrait[interiorFieldCount];
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

        IRTreeFrameFactory interiorFrameFactory = new NSMRTreeFrameFactory(interiorTupleWriterFactory, keyFieldCount);
        IRTreeFrameFactory leafFrameFactory = new NSMRTreeFrameFactory(leafTupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IRTreeFrame interiorFrame = interiorFrameFactory.getFrame();
        IRTreeFrame leafFrame = leafFrameFactory.getFrame();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

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

        RTreeOpContext insertOpCtx = rtree.createOpContext(IndexOp.INSERT, interiorFrame, leafFrame, metaFrame,
                "unittest");

        Random rnd = new Random();
        rnd.setSeed(50);
        Stack<Integer> s = new Stack<Integer>();
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

            long end = System.currentTimeMillis();
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

        RTreeOpContext searchOpCtx = rtree.createOpContext(IndexOp.SEARCH, interiorFrame, leafFrame, metaFrame,
                "unittest");
        ArrayList<Rectangle> results = new ArrayList<Rectangle>();
        rtree.search(s, tuple, searchOpCtx, results);

        // for (int i = 0; i < results.size(); i++) {
        // for (int j = 0; j < dim; j++) {
        // System.out.print(results.get(i).getLow(j) + " " +
        // results.get(i).getHigh(j));
        // }
        // System.out.println();
        // }
        System.out.println("Number of Results: " + results.size());

        String stats = rtree.printStats();
        print(stats);

        rtree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();

    }

    // @Test
    public void test02() throws Exception {

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare interior-frame-tuple fields
        int interiorFieldCount = 5;
        ITypeTrait[] interiorTypeTraits = new ITypeTrait[interiorFieldCount];
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

        IRTreeFrameFactory interiorFrameFactory = new NSMRTreeFrameFactory(interiorTupleWriterFactory, keyFieldCount);
        IRTreeFrameFactory leafFrameFactory = new NSMRTreeFrameFactory(leafTupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IRTreeFrame interiorFrame = interiorFrameFactory.getFrame();
        IRTreeFrame leafFrame = leafFrameFactory.getFrame();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

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

        RTreeOpContext insertOpCtx = rtree.createOpContext(IndexOp.INSERT, interiorFrame, leafFrame, metaFrame,
                "unittest");

        File datasetFile = new File("/home/salsubaiee/dataset.txt");
        BufferedReader reader = new BufferedReader(new FileReader(datasetFile));

        Random rnd = new Random();
        rnd.setSeed(50);
        String inputLine = reader.readLine();
        int index = 0;

        while (inputLine != null) {

            String[] splittedLine1 = inputLine.split(",");
            String[] splittedLine2 = splittedLine1[0].split("\\s");

            double p1x = 0;
            double p1y = 0;

            try {
                p1x = Double.valueOf(splittedLine2[1].trim()).doubleValue();
                p1y = Double.valueOf(splittedLine2[2].trim()).doubleValue();
            } catch (Exception e) {
                inputLine = reader.readLine();
                continue;
            }
            double p2x = p1x;
            double p2y = p1y;

            int pk = rnd.nextInt();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1y, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2y, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk, dos);
            tb.addFieldEndOffset();

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            long end = System.currentTimeMillis();
            print("INSERTING " + index + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
                    + " " + Math.max(p1y, p2y) + "\n");

            try {
                rtree.insert(tuple, insertOpCtx);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (index == 1000) {
                break;
            }
            inputLine = reader.readLine();
            index++;

            // rtree.printTree(leafFrame, interiorFrame, recDescSers);
            // System.out.println();
        }

        // rtree.printTree(leafFrame, interiorFrame, recDescSers);
        // System.out.println();

        RTreeOpContext searchOpCtx = rtree.createOpContext(IndexOp.SEARCH, interiorFrame, leafFrame, metaFrame,
                "unittest");

        File querysetFile = new File("/home/salsubaiee/queryset.txt");
        BufferedReader reader2 = new BufferedReader(new FileReader(querysetFile));

        inputLine = reader2.readLine();
        int totalResults = 0;
        index = 0;
        Stack<Integer> s = new Stack<Integer>();
        while (inputLine != null) {

            String[] splittedLine1 = inputLine.split(",");
            String[] splittedLine2 = splittedLine1[0].split("\\s");

            double p1x;
            double p1y;
            double p2x;
            double p2y;

            try {
                p1x = Double.valueOf(splittedLine2[1].trim()).doubleValue();
                p1y = Double.valueOf(splittedLine2[2].trim()).doubleValue();
                p2x = Double.valueOf(splittedLine2[3].trim()).doubleValue();
                p2y = Double.valueOf(splittedLine2[4].trim()).doubleValue();
            } catch (Exception e) {
                inputLine = reader2.readLine();
                continue;
            }

            int pk = rnd.nextInt();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1y, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2y, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk, dos);
            tb.addFieldEndOffset();

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            long end = System.currentTimeMillis();
            print("SEARCHING " + index + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
                    + " " + Math.max(p1y, p2y) + "\n");

            try {
                ArrayList<Rectangle> results = new ArrayList<Rectangle>();
                rtree.search(s, tuple, searchOpCtx, results);
                totalResults += results.size();
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }

            inputLine = reader2.readLine();
            index++;

        }

        System.out.println("Number of Results: " + totalResults);

        // String stats = rtree.printStats();
        // print(stats);

        RTreeOpContext deleteOpCtx = rtree.createOpContext(IndexOp.DELETE, interiorFrame, leafFrame, metaFrame,
                "unittest");

        BufferedReader reader3 = new BufferedReader(new FileReader(datasetFile));
        inputLine = reader3.readLine();
        index = 0;
        rnd.setSeed(50);
        while (inputLine != null) {

            String[] splittedLine1 = inputLine.split(",");
            String[] splittedLine2 = splittedLine1[0].split("\\s");

            double p1x = 0;
            double p1y = 0;

            try {
                p1x = Double.valueOf(splittedLine2[1].trim()).doubleValue();
                p1y = Double.valueOf(splittedLine2[2].trim()).doubleValue();
            } catch (Exception e) {
                inputLine = reader3.readLine();
                continue;
            }
            double p2x = p1x;
            double p2y = p1y;

            int pk = rnd.nextInt();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1y, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2y, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk, dos);
            tb.addFieldEndOffset();

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            print("Deleteing " + index + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
                    + " " + Math.max(p1y, p2y) + "\n");

            try {
                rtree.delete(tuple, deleteOpCtx);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (index == 1000) {
                break;
            }
            inputLine = reader3.readLine();
            index++;

            // rtree.printTree(leafFrame, interiorFrame, recDescSers);
            // System.out.println();
        }

        BufferedReader reader4 = new BufferedReader(new FileReader(querysetFile));

        inputLine = reader4.readLine();
        totalResults = 0;
        index = 0;
        Stack<Integer> s2 = new Stack<Integer>();
        while (inputLine != null) {

            String[] splittedLine1 = inputLine.split(",");
            String[] splittedLine2 = splittedLine1[0].split("\\s");

            double p1x = Double.valueOf(splittedLine2[1].trim()).doubleValue();
            double p1y = Double.valueOf(splittedLine2[2].trim()).doubleValue();
            double p2x = Double.valueOf(splittedLine2[3].trim()).doubleValue();
            double p2y = Double.valueOf(splittedLine2[4].trim()).doubleValue();

            int pk = rnd.nextInt();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1y, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2y, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk, dos);
            tb.addFieldEndOffset();

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            print("SEARCHING " + index + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
                    + " " + Math.max(p1y, p2y) + "\n");

            try {
                ArrayList<Rectangle> results = new ArrayList<Rectangle>();
                rtree.search(s2, tuple, searchOpCtx, results);
                totalResults += results.size();
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }

            inputLine = reader4.readLine();
            index++;

        }

        System.out.println("Number of Results: " + totalResults);

        // stats = rtree.printStats();
        // print(stats);

        rtree.printTree(leafFrame, interiorFrame, recDescSers);
        System.out.println();

        rtree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();

    }
}
