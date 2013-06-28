/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.util.AbstractBTreeTest;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.util.TreeIndexUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class FieldPrefixNSMTest extends AbstractBTreeTest {

    private static final int PAGE_SIZE = 32768;
    private static final int NUM_PAGES = 40;
    private static final int MAX_OPEN_FILES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;

    public FieldPrefixNSMTest() {
        super(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES, HYRACKS_FRAME_SIZE);
    }

    private ITupleReference createTuple(IHyracksTaskContext ctx, int f0, int f1, int f2, boolean print)
            throws HyracksDataException {
        if (print) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("CREATING: " + f0 + " " + f1 + " " + f2);
            }
        }

        ByteBuffer buf = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(3);
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(buf);
        FrameTupleReference tuple = new FrameTupleReference();

        tb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
        tb.addFieldEndOffset();
        IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
        tb.addFieldEndOffset();
        IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
        tb.addFieldEndOffset();

        appender.reset(buf, true);
        appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

        tuple.reset(accessor, 0);

        return tuple;
    }

    @Test
    public void test01() throws Exception {

        // declare fields
        int fieldCount = 3;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;

        // declare keys
        int keyFieldCount = 3;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        cmps[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        cmps[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        MultiComparator cmp = new MultiComparator(cmps);

        // just for printing
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        Random rnd = new Random();
        rnd.setSeed(50);

        IBufferCache bufferCache = harness.getBufferCache();
        IFileMapProvider fileMapProvider = harness.getFileMapProvider();
        bufferCache.createFile(harness.getFileReference());
        int btreeFileId = fileMapProvider.lookupFileId(harness.getFileReference());
        bufferCache.openFile(btreeFileId);
        IHyracksTaskContext ctx = harness.getHyracksTaskContext();
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(btreeFileId, 0), false);
        try {

            ITreeIndexTupleWriter tupleWriter = new TypeAwareTupleWriter(typeTraits);
            BTreeFieldPrefixNSMLeafFrame frame = new BTreeFieldPrefixNSMLeafFrame(tupleWriter);
            frame.setPage(page);
            frame.initBuffer((byte) 0);
            frame.setMultiComparator(cmp);
            frame.setPrefixTupleCount(0);

            String before = new String();
            String after = new String();

            int compactFreq = 5;
            int compressFreq = 5;
            int smallMax = 10;
            int numRecords = 1000;

            int[][] savedFields = new int[numRecords][3];

            // insert records with random calls to compact and compress
            for (int i = 0; i < numRecords; i++) {

                if (LOGGER.isLoggable(Level.INFO)) {
                    if ((i + 1) % 100 == 0) {
                        LOGGER.info("INSERTING " + (i + 1) + " / " + numRecords);
                    }
                }

                int a = rnd.nextInt() % smallMax;
                int b = rnd.nextInt() % smallMax;
                int c = i;

                ITupleReference tuple = createTuple(ctx, a, b, c, false);
                try {
                    int targetTupleIndex = frame.findInsertTupleIndex(tuple);
                    frame.insert(tuple, targetTupleIndex);
                } catch (BTreeException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                savedFields[i][0] = a;
                savedFields[i][1] = b;
                savedFields[i][2] = c;

                if (rnd.nextInt() % compactFreq == 0) {
                    before = TreeIndexUtils.printFrameTuples(frame, fieldSerdes);
                    frame.compact();
                    after = TreeIndexUtils.printFrameTuples(frame, fieldSerdes);
                    Assert.assertEquals(before, after);
                }

                if (rnd.nextInt() % compressFreq == 0) {
                    before = TreeIndexUtils.printFrameTuples(frame, fieldSerdes);
                    frame.compress();
                    after = TreeIndexUtils.printFrameTuples(frame, fieldSerdes);
                    Assert.assertEquals(before, after);
                }

            }

            // delete records with random calls to compact and compress
            for (int i = 0; i < numRecords; i++) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    if ((i + 1) % 100 == 0) {
                        LOGGER.info("DELETING " + (i + 1) + " / " + numRecords);
                    }
                }

                ITupleReference tuple = createTuple(ctx, savedFields[i][0], savedFields[i][1], savedFields[i][2], false);
                try {
                    int tupleIndex = frame.findDeleteTupleIndex(tuple);
                    frame.delete(tuple, tupleIndex);
                } catch (Exception e) {
                }

                if (rnd.nextInt() % compactFreq == 0) {
                    before = TreeIndexUtils.printFrameTuples(frame, fieldSerdes);
                    frame.compact();
                    after = TreeIndexUtils.printFrameTuples(frame, fieldSerdes);
                    Assert.assertEquals(before, after);
                }

                if (rnd.nextInt() % compressFreq == 0) {
                    before = TreeIndexUtils.printFrameTuples(frame, fieldSerdes);
                    frame.compress();
                    after = TreeIndexUtils.printFrameTuples(frame, fieldSerdes);
                    Assert.assertEquals(before, after);
                }
            }

        } finally {
            bufferCache.unpin(page);
            bufferCache.closeFile(btreeFileId);
            bufferCache.close();
        }
    }
}
