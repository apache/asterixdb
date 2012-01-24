package edu.uci.ics.hyracks.storage.am.lsmtree.btree;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;

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
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.freepage.FreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.freepage.InMemoryBufferCacheFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.LSMTree;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.LSMTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsmtree.tuples.LSMTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class LSMTreeDeleteTest extends AbstractLSMTreeTest {

    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 100;
    private static final int MAX_OPEN_FILES = 100;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    // BASIC DELETE TEST
    // create a fix-length lsm tree, and do 100 deletes. That is insert 100
    // delete nodes into the in-memory tree.
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

        IFreePageManager memFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);

        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                interiorFrameFactory, insertLeafFrameFactory);

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

        int resultSize = 50;
        int[][] resultArray = new int[resultSize][3];

        for (int i = 0; i < resultSize; i++) {
            resultArray[i][0] = i;
            resultArray[i][1] = i + 1;
            resultArray[i][2] = 1;
        }

        // delete
        for (int i = 0; i < resultSize; i++) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

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

        // scan
        ITreeIndexCursor scanCursor = new LSMTreeRangeSearchCursor();
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        lsmTreeAccessor.search(scanCursor, nullPred);

        try {
            int scanTupleIndex = 0;
            int arrayIndex = 0;
            Object o = null;
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                int numPrintFields = Math.min(frameTuple.getFieldCount(), recDescSers.length);

                for (int i = 0; i < numPrintFields; i++) {
                    ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(i),
                            frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    DataInput dataIn = new DataInputStream(inStream);
                    o = recDescSers[i].deserialize(dataIn);

                }
                while (resultArray[arrayIndex][2] != 0) {
                    arrayIndex++;
                }
                if (Integer.parseInt(o.toString()) != resultArray[arrayIndex][1]) {
                    fail("Input value and Output value doesn't match on the " + scanTupleIndex + " tuple\n");
                }
                scanTupleIndex++;
                arrayIndex++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        lsmtree.close();
        bufferCache.closeFile(fileId);
        memBufferCache.close();
    }

    // INSERT-DELETE TEST
    // create a fix-length lsm tree. First, do 100 insertions,
    // and then do 50 deletions which has the same 50 keys which are part of the
    // insertions.
    @Test
    public void Test2() throws Exception {
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

        IFreePageManager memFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);

        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                interiorFrameFactory, insertLeafFrameFactory);

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

        int resultSize = 100;
        int deleteStartPosition = 50;
        int[][] resultArray = new int[resultSize][3];

        for (int i = 0; i < resultSize; i++) {
            resultArray[i][0] = i;
            resultArray[i][1] = i + 1;
            resultArray[i][2] = 0;
        }

        // insert
        ITreeIndexAccessor lsmTreeAccessor = lsmtree.createAccessor();
        for (int i = 0; i < resultSize; i++) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

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
                System.out.println("test02:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // delete
        for (int i = deleteStartPosition; i < resultSize; i++) {

            int f0 = resultArray[i][0];
            int f1 = ++resultArray[i][1];
            resultArray[i][2] = 1;

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
                System.out.println("test02:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // scan
        ITreeIndexCursor scanCursor = new LSMTreeRangeSearchCursor();
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        lsmTreeAccessor.search(scanCursor, nullPred);

        try {
            int scanTupleIndex = 0;
            int arrayIndex = 0;
            Object o = null;
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                int numPrintFields = Math.min(frameTuple.getFieldCount(), recDescSers.length);

                for (int i = 0; i < numPrintFields; i++) {
                    ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(i),
                            frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    DataInput dataIn = new DataInputStream(inStream);
                    o = recDescSers[i].deserialize(dataIn);

                }
                while (resultArray[arrayIndex][2] != 0) {
                    arrayIndex++;
                }
                if (Integer.parseInt(o.toString()) != resultArray[arrayIndex][1]) {
                    fail("Input value and Output value doesn't match on the " + scanTupleIndex + " tuple\n");
                }

                scanTupleIndex++;
                arrayIndex++;

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        lsmtree.close();
        bufferCache.closeFile(fileId);
        memBufferCache.close();
    }

    // DELETE->INSERT TEST
    // create a fix-length lsm tree. First, do 100 deletions,
    // and then do 50 insertions which has the same 50 keys which are part of
    // the deletions.
    @Test
    public void Test3() throws Exception {
        System.out.println("TEST3");
        // in disk
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // in mem
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
        // change
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IFreePageManager memFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);

        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                interiorFrameFactory, insertLeafFrameFactory);

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

        int resultSize = 100;
        int insertStartPosition = 50;
        int[][] resultArray = new int[resultSize][3];

        for (int i = 0; i < resultSize; i++) {
            resultArray[i][0] = i;
            resultArray[i][1] = i + 1;
            resultArray[i][2] = 1;
        }

        // delete
        ITreeIndexAccessor lsmTreeAccessor = lsmtree.createAccessor();
        for (int i = 0; i < resultSize; i++) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

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
                System.out.println("test03:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // insert
        for (int i = insertStartPosition; i < resultSize; i++) {

            int f0 = resultArray[i][0];
            int f1 = ++resultArray[i][1];
            resultArray[i][2] = 0;

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
                System.out.println("test03:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // scan
        ITreeIndexCursor scanCursor = new LSMTreeRangeSearchCursor();
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        lsmTreeAccessor.search(scanCursor, nullPred);

        try {
            int scanTupleIndex = 0;
            int arrayIndex = 0;
            Object o = null;
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                int numPrintFields = Math.min(frameTuple.getFieldCount(), recDescSers.length);

                for (int i = 0; i < numPrintFields; i++) {
                    ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(i),
                            frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    DataInput dataIn = new DataInputStream(inStream);
                    o = recDescSers[i].deserialize(dataIn);
                }
                while (resultArray[arrayIndex][2] != 0) {
                    arrayIndex++;
                }
                if (Integer.parseInt(o.toString()) != resultArray[arrayIndex][1]) {
                    fail("Input value and Output value doesn't match on the " + scanTupleIndex + " tuple\n");
                }

                scanTupleIndex++;
                arrayIndex++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        lsmtree.close();
        bufferCache.closeFile(fileId);
        memBufferCache.close();
    }

    // TEST DELETION and PageAllocationException
    // create a fix-length lsm tree. First, do 811 deletions,
    // the page will be run out on the 810th deletions, if there is any
    // exception returns, the test case fails.
    @Test
    public void Test4() throws Exception {
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

        IFreePageManager memFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);

        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                interiorFrameFactory, insertLeafFrameFactory);

        // For the Flush Mechanism
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

        int resultSize = 811;
        int[][] resultArray = new int[resultSize][2];

        for (int i = 0; i < resultSize; i++) {
            resultArray[i][0] = i;
            resultArray[i][1] = i + 1;
        }

        // delete
        ITreeIndexAccessor lsmTreeAccessor = lsmtree.createAccessor();
        for (int i = 0; i < resultSize; i++) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

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
                System.out.println("test04:" + e);
                e.printStackTrace();
                fail("test04: Catch TreeIndexException" + e);
            } catch (Exception e) {
                e.printStackTrace();
                fail("test04: Catch Other Exceptions" + e);
            }
        }
    }

    // DELETE -> DELETE
    // create a fix-length lsm tree. First, do 100 deletions,
    // and then do 50 deletions which has the same 50 keys which are part of the
    // first deletions.
    @Test
    public void Test5() throws Exception {
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

        IFreePageManager memFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);

        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                interiorFrameFactory, insertLeafFrameFactory);

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

        int resultSize = 100;
        int insertStartPosition = 50;
        int[][] resultArray = new int[resultSize][3];

        for (int i = 0; i < resultSize; i++) {
            resultArray[i][0] = i;
            resultArray[i][1] = i + 1;
            resultArray[i][2] = 1;
        }

        // First deletion part
        ITreeIndexAccessor lsmTreeAccessor = lsmtree.createAccessor();
        for (int i = 0; i < resultSize; i++) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

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
                System.out.println("test05:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Second delete part
        for (int i = insertStartPosition; i < resultSize; i++) {

            int f0 = resultArray[i][0];
            int f1 = ++resultArray[i][1];
            resultArray[i][2] = 1;

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
                System.out.println("test05:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // scan
        ITreeIndexCursor scanCursor = new LSMTreeRangeSearchCursor();
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        lsmTreeAccessor.search(scanCursor, nullPred);

        try {
            int scanTupleIndex = 0;
            int arrayIndex = 0;
            Object o = null;
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                int numPrintFields = Math.min(frameTuple.getFieldCount(), recDescSers.length);

                for (int i = 0; i < numPrintFields; i++) {
                    ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(i),
                            frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    DataInput dataIn = new DataInputStream(inStream);
                    o = recDescSers[i].deserialize(dataIn);

                }
                while (resultArray[arrayIndex][2] != 0) {
                    arrayIndex++;
                }
                if (Integer.parseInt(o.toString()) != resultArray[arrayIndex][1]) {
                    fail("Input value and Output value doesn't match on the " + scanTupleIndex + " tuple\n");
                }

                scanTupleIndex++;
                arrayIndex++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        lsmtree.close();
        bufferCache.closeFile(fileId);
        memBufferCache.close();
    }

    // INSERT -> DELETE -> INSERT
    // create a fix-length lsm tree. Do the insertion, deletion and insertion.
    // the final result will be
    // | 0~9 | 10~19 | 20~39 | 40~59 | 60~79 | 80~99 |
    // | f1=10 | f1=9 | f1=8 | f1=7 | f1=6 | f1=5 |
    // | Insert| Insert| Delete| Delete| Insert| Insert|
    @Test
    public void Test6() throws Exception {

        // in disk
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // in mem
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

        IFreePageManager memFreePageManager = new InMemoryFreePageManager(100, metaFrameFactory);

        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                interiorFrameFactory, insertLeafFrameFactory);

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

        int resultSize = 180;
        int[][] resultArray = new int[resultSize][3];

        // insert
        for (int i = 0; i < 180; i++) {
            int f0 = i % 100;
            int f1;
            if (i >= 100) {
                f1 = 6;
            } else {
                f1 = 5;
            }

            resultArray[f0][0] = f0;
            resultArray[f0][1] = f1;
            resultArray[f0][2] = 0;

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
                System.out.println("test06:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // delete
        for (int i = 0; i < 100; i++) {
            int f0 = i % 60;
            int f1;
            if (i >= 60) {
                f1 = 8;
            } else {
                f1 = 7;
            }

            resultArray[f0][0] = f0;
            resultArray[f0][1] = f1;
            resultArray[f0][2] = 1;

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
                System.out.println("test06:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        // reinsert
        for (int i = 0; i < 30; i++) {
            int f0 = i % 20;
            int f1;
            if (i >= 20) {
                f1 = 10;
            } else {
                f1 = 9;
            }

            resultArray[f0][0] = f0;
            resultArray[f0][1] = f1;
            resultArray[f0][2] = 0;

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
                System.out.println("test06:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // scan
        ITreeIndexCursor scanCursor = new LSMTreeRangeSearchCursor();
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        lsmTreeAccessor.search(scanCursor, nullPred);

        try {
            int scanTupleIndex = 0;
            int arrayIndex = 0;
            Object o = null;
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                int numPrintFields = Math.min(frameTuple.getFieldCount(), recDescSers.length);

                for (int i = 0; i < numPrintFields; i++) {
                    ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(i),
                            frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    DataInput dataIn = new DataInputStream(inStream);
                    o = recDescSers[i].deserialize(dataIn);
                }
                while (resultArray[arrayIndex][2] != 0) {
                    arrayIndex++;
                }
                if (Integer.parseInt(o.toString()) != resultArray[arrayIndex][1]) {
                    fail("Input value and Output value doesn't match on the " + scanTupleIndex + " tuple\n");
                }

                scanTupleIndex++;
                arrayIndex++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        lsmtree.close();
        bufferCache.closeFile(fileId);
        memBufferCache.close();
    }
}
