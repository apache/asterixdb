/*package edu.uci.ics.hyracks.storage.am.rtree;

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
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.SpaceStatus;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleWriter;
import edu.uci.ics.hyracks.storage.am.rtree.frames.NSMRTreeFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeFrameTest extends AbstractRTreeTest {

    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksStageletContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    private String tmpDir = System.getProperty("java.io.tmpdir");

    @Test
    public void frameInsertTest() throws Exception {

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder
                .getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder
                .getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 5;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(8);
        typeTraits[1] = new TypeTrait(8);
        typeTraits[2] = new TypeTrait(8);
        typeTraits[3] = new TypeTrait(8);
        typeTraits[4] = new TypeTrait(4);

        // declare keys
        int keyFieldCount = 4;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = DoubleBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = cmps[0];
        cmps[2] = cmps[0];
        cmps[3] = cmps[0];

        MultiComparator cmp = new MultiComparator(typeTraits, cmps);

        TypeAwareTupleWriter tupleWriter = new TypeAwareTupleWriter(typeTraits);
        //SimpleTupleWriter tupleWriter = new SimpleTupleWriter();
        NSMRTreeFrame rtreeFrame = new NSMRTreeFrame(tupleWriter);

        ByteBuffer hyracksFrame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx
                .getFrameSize(), recDesc);
        FrameTupleReference tuple = new FrameTupleReference();

        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, 0), true);
        try {

            rtreeFrame.setPage(page);
            rtreeFrame.initBuffer((byte)0);

            // insert some random stuff...

            Random rnd = new Random(50);

            int numInserts = 10;
            for (int i = 0; i < numInserts; i++) {
                double p1x = rnd.nextDouble();
                double p1y = rnd.nextDouble();
                double p2x = rnd.nextDouble();
                double p2y = rnd.nextDouble();

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

                SpaceStatus status = rtreeFrame.hasSpaceInsert(tuple, cmp);
                switch(status) {
                    case SUFFICIENT_CONTIGUOUS_SPACE: {
                        rtreeFrame.insert(tuple, cmp);
                        break;
                    }
                
                    case SUFFICIENT_SPACE: {
                        rtreeFrame.compact(cmp);
                        rtreeFrame.insert(tuple, cmp);
                        break;
                    }
                    
                    case INSUFFICIENT_SPACE: {
                        // split
                        System.out.println("PLEASE STOP, NO MORE SPACE");
                        break;
                    }
                }
                
                

                String contents = rtreeFrame.printKeys(cmp, recDescSers);
                System.out.println(contents);

            }
        } finally {
            bufferCache.unpin(page);
        }

        bufferCache.closeFile(fileId);
        bufferCache.close();

    }

}
*/