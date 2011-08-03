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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.Random;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeWriterTest extends Thread {
    private RTree rtree;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksStageletContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    public RTreeWriterTest(RTree rtree, IHyracksStageletContext ctx, String threadName) {
        this.rtree = rtree;
        this.setName(threadName);
    }

    private boolean keepRunning = true;

    @Override
    public void run() {
        // while (keepRunning) {

        // declare keys
        int keyFieldCount = 4;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = DoubleBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = cmps[0];
        cmps[2] = cmps[0];
        cmps[3] = cmps[0];

        // declare tuple fields
        int fieldCount = 5;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(8);
        typeTraits[1] = new TypeTrait(8);
        typeTraits[2] = new TypeTrait(8);
        typeTraits[3] = new TypeTrait(8);
        typeTraits[4] = new TypeTrait(4);

        MultiComparator cmp = new MultiComparator(typeTraits, cmps);

        RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(typeTraits);

        ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(tupleWriterFactory,
                keyFieldCount);
        ITreeIndexFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(tupleWriterFactory, keyFieldCount);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

        IRTreeFrame interiorFrame = (IRTreeFrame) interiorFrameFactory.createFrame();
        IRTreeFrame leafFrame = (IRTreeFrame) leafFrameFactory.createFrame();

        ByteBuffer hyracksFrame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(hyracksFrame);
        FrameTupleReference tuple = new FrameTupleReference();

        RTreeOpContext insertOpCtx = rtree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);

        RTreeOpContext deleteOpCtx = rtree.createOpContext(IndexOp.DELETE, leafFrame, interiorFrame, metaFrame);

        Random rnd = new Random();
        rnd.setSeed(50);
        for (int i = 0; i < 10000; i++) {

            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();

            int pk = rnd.nextInt();

            tb.reset();
            try {
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
            } catch (HyracksDataException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            System.out.println(this.getName() + " INSERTING " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y)
                    + " " + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y) + "\n");

            try {
                rtree.insert(tuple, insertOpCtx);

            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();

                try {
                    rtree.printTree(leafFrame, interiorFrame, recDescSers);
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                // System.out.println();
            }

        }

        rnd.setSeed(50);
        for (int i = 0; i < 10000; i++) {

            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();

            int pk = rnd.nextInt();

            tb.reset();
            try {
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
            } catch (HyracksDataException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            System.out.println(this.getName() + " Deleteing " + i + " " + Math.min(p1x, p2x) + " " + Math.min(p1y, p2y)
                    + " " + Math.max(p1x, p2x) + " " + Math.max(p1y, p2y) + "\n");

            try {
                rtree.delete(tuple, deleteOpCtx);

            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();

                try {
                    rtree.printTree(leafFrame, interiorFrame, recDescSers);
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                // System.out.println();
            }
        }

        // update every seconds
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // }
    }

    public void stopWriter() {
        this.keepRunning = false;
        this.interrupt();
    }

}