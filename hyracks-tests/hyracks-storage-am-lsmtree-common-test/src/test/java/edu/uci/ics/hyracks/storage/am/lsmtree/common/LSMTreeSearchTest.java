package edu.uci.ics.hyracks.storage.am.lsmtree.btree;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.freepage.FreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.freepage.InMemoryBufferCacheFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.InDiskTreeInfo;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.LSMTree;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.LSMTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsmtree.tuples.LSMEntireTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.tuples.LSMTypeAwareTupleReference;
import edu.uci.ics.hyracks.storage.am.lsmtree.tuples.LSMTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

// TODO: needs a big cleanup phase.
public class LSMTreeSearchTest extends AbstractLSMTreeTest {

    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int MAX_OPEN_FILES = 100;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

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
        InMemoryBufferCacheFactory InMemBufferCacheFactory = new InMemoryBufferCacheFactory(PAGE_SIZE, NUM_PAGES);
        IBufferCache memBufferCache = InMemBufferCacheFactory.createInMemoryBufferCache();

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();

        MultiComparator cmp = new MultiComparator(cmps);

        LSMTypeAwareTupleWriterFactory insertTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        LSMTypeAwareTupleWriterFactory deleteTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame insertLeafFrame = (IBTreeLeafFrame) insertLeafFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);
        IFreePageManager memFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);

        LSMEntireTupleWriterFactory flushTupleWriterFactory = new LSMEntireTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory flushLeafFrameFactory = new BTreeNSMLeafFrameFactory(flushTupleWriterFactory);
        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                interiorFrameFactory, flushLeafFrameFactory);

        LSMTree lsmtree = new LSMTree(memBufferCache, bufferCache, fieldCount, cmp, memFreePageManager,
                interiorFrameFactory, insertLeafFrameFactory, deleteLeafFrameFactory, bTreeFactory,
                (IFileMapManager) fmp);

        lsmtree.create(fileId);
        lsmtree.open(fileId);

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

        ITreeIndexAccessor lsmTreeAccessor = lsmtree.createAccessor();

        // delete
        for (int i = 26; i < 36; i++) {

            int f0 = i;
            int f1 = -1;

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

            try {
                lsmTreeAccessor.delete(t);
            } catch (TreeIndexException e) {
                System.out.println("test01:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (int i = 21; i < 31; i++) {
            int f0 = i;
            int f1 = 0;

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

            try {
                lsmTreeAccessor.insert(t);
            } catch (TreeIndexException e) {
                System.out.println("test01:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // In disk insertions 1

        LOGGER.info("Start in-disk insertions");

        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        FileReference file_1 = new FileReference(new File(fileName));
        bufferCache.createFile(file_1);
        int fileId_1 = fmp.lookupFileId(file_1);
        bufferCache.openFile(fileId_1);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IFreePageManager freePageManager_1 = new LinkedListFreePageManager(bufferCache, fileId_1, 0, metaFrameFactory);

        BTree btree_1 = new BTree(bufferCache, fieldCount, cmp, freePageManager_1, interiorFrameFactory,
                leafFrameFactory);
        btree_1.create(fileId_1);
        btree_1.open(fileId_1);

        // TODO: rename this one.
        InDiskTreeInfo info_1 = new InDiskTreeInfo(btree_1);
        lsmtree.inDiskTreeInfoList.add(info_1);

        Random rnd = new Random();
        rnd.setSeed(50);

        LOGGER.info("INSERTING INTO TREE");

        // ByteBuffer
        frame = ctx.allocateFrame();
        // FrameTupleAppender
        appender = new FrameTupleAppender(ctx.getFrameSize());
        // ArrayTupleBuilder
        tb = new ArrayTupleBuilder(fieldCount);
        // DataOutput
        dos = tb.getDataOutput();

        recDesc = new RecordDescriptor(recDescSers);

        accessor.reset(frame);

        tuple = new FrameTupleReference();

        ITreeIndexAccessor indexAccessor_1 = btree_1.createAccessor();

        // 10000
        for (int i = 16; i < 41; i++) {

            int f0 = i;
            int f1 = 1;

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

            if (i % 10 == 0) {
                System.out.println("INSERTING " + i + " : " + f0 + " " + f1);
            }

            try {
                indexAccessor_1.insert(t);
            } catch (TreeIndexException e) {
                e.printStackTrace();
                System.out.println("Error: " + e);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // btree_1.close();

        // In disk insertions 2

        LOGGER.info("Start in-disk insertions");

        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        FileReference file_2 = new FileReference(new File(fileName));
        bufferCache.createFile(file_2);
        int fileId_2 = fmp.lookupFileId(file_2);
        bufferCache.openFile(fileId_2);

        IFreePageManager freePageManager_2 = new LinkedListFreePageManager(bufferCache, fileId_2, 0, metaFrameFactory);
        BTree btree_2 = new BTree(bufferCache, fieldCount, cmp, freePageManager_2, interiorFrameFactory,
                leafFrameFactory);
        btree_2.create(fileId_2);
        btree_2.open(fileId_2);

        InDiskTreeInfo info_2 = new InDiskTreeInfo(btree_2);
        lsmtree.inDiskTreeInfoList.add(info_2);

        LOGGER.info("INSERTING INTO TREE");

        // ByteBuffer
        frame = ctx.allocateFrame();
        // FrameTupleAppender
        appender = new FrameTupleAppender(ctx.getFrameSize());
        // ArrayTupleBuilder
        tb = new ArrayTupleBuilder(fieldCount);
        // DataOutput
        dos = tb.getDataOutput();

        recDesc = new RecordDescriptor(recDescSers);

        accessor.reset(frame);

        tuple = new FrameTupleReference();

        ITreeIndexAccessor indexAccessor_2 = btree_2.createAccessor();

        // 10000
        for (int i = 11; i < 51; i++) {

            int f0 = i;
            int f1 = 2;

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

            if (i % 10 == 0) {
                System.out.println("INSERTING " + i + " : " + f0 + " " + f1);
            }

            try {
                indexAccessor_2.insert(t);
            } catch (TreeIndexException e) {
                e.printStackTrace();
                System.out.println("Error: " + e);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // btree_2.close();

        // range search in [-1000, 1000]
        LOGGER.info("RANGE SEARCH:");

        ITreeIndexCursor rangeCursor = new LSMTreeRangeSearchCursor();

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
        IntegerSerializerDeserializer.INSTANCE.serialize(-100, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // build and append high key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(100, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // create tuplereferences for search keys
        FrameTupleReference lowKey = new FrameTupleReference();
        lowKey.reset(keyAccessor, 0);

        FrameTupleReference highKey = new FrameTupleReference();
        highKey.reset(keyAccessor, 1);

        IBinaryComparator[] searchCmps = cmps;
        MultiComparator searchCmp = new MultiComparator(searchCmps);

        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, searchCmp, searchCmp);
        lsmTreeAccessor.search(rangeCursor, rangePred);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, recDescSers);
                if (((LSMTypeAwareTupleReference) frameTuple).isDelete()) {
                    System.out.println("del " + rec);
                } else {
                    System.out.println("ins " + rec);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        lsmtree.close();
        bufferCache.closeFile(fileId);
        memBufferCache.close();
    }
}