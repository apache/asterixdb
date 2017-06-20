/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.runtime.operators.LSMSecondaryIndexCreationTupleProcessorNodePushable.DeletedTupleCounter;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class LSMSecondaryIndexCreationTupleProcessorTest {

    private static final int FRAME_SIZE = 512;

    private final ISerializerDeserializer[] fields =
            { IntegerSerializerDeserializer.INSTANCE, BooleanSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

    private final RecordDescriptor recDesc = new RecordDescriptor(fields);

    private final int numTagFields = 2;

    private final int numSecondaryKeys = 1;

    private final int numPrimaryKeys = 1;

    private class ResultFrameWriter implements IFrameWriter {
        FrameTupleAccessor resultAccessor = new FrameTupleAccessor(recDesc);

        FrameTupleReference tuple = new FrameTupleReference();
        final List<ITupleReference> resultTuples;

        public ResultFrameWriter(List<ITupleReference> resultTuples) {
            this.resultTuples = resultTuples;
        }

        @Override
        public void open() throws HyracksDataException {
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            resultAccessor.reset(buffer);
            int count = resultAccessor.getTupleCount();
            for (int i = 0; i < count; i++) {
                tuple.reset(resultAccessor, i);
                resultTuples.add(TupleUtils.copyTuple(tuple));
            }
        }

        @Override
        public void fail() throws HyracksDataException {

        }

        @Override
        public void close() throws HyracksDataException {
        }
    }

    @Test
    public void testBasic() {
        List<Object[]> inputs = new ArrayList<>();
        inputs.add(new Object[] { 0, false, 1, 1 });
        inputs.add(new Object[] { 1, false, 1, 2 });
        inputs.add(new Object[] { 2, false, 2, 3 });

        List<Object[]> outputs = new ArrayList<>();
        outputs.add(new Object[] { 0, false, 1, 1 });
        outputs.add(new Object[] { 1, false, 1, 2 });
        outputs.add(new Object[] { 2, false, 2, 3 });

        List<Object[]> buddyOutputs = new ArrayList<>();
        buddyOutputs.add(new Object[] { 0, false, 1, 1 });
        buddyOutputs.add(new Object[] { 1, false, 1, 2 });
        buddyOutputs.add(new Object[] { 2, false, 2, 3 });
        try {
            runTest(inputs, outputs, new int[] { 0, 0, 0 }, false);
            runTest(inputs, buddyOutputs, new int[] { 0, 0, 0 }, true);
        } catch (HyracksDataException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testUpsertWithSameSecondary() {
        List<Object[]> inputs = new ArrayList<>();
        inputs.add(new Object[] { 1, false, 1, 1 });
        inputs.add(new Object[] { 0, false, 1, 1 });

        List<Object[]> outputs = new ArrayList<>();
        outputs.add(new Object[] { 1, false, 1, 1 });
        outputs.add(new Object[] { 0, false, 1, 1 });

        List<Object[]> buddyOutputs = new ArrayList<>();
        buddyOutputs.add(new Object[] { 1, false, 1, 1 });
        buddyOutputs.add(new Object[] { 0, true, null, 1 });
        buddyOutputs.add(new Object[] { 0, false, 1, 1 });

        try {
            runTest(inputs, outputs, new int[] { 0, 0 }, false);
            runTest(inputs, buddyOutputs, new int[] { 1, 0 }, true);
        } catch (HyracksDataException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testUpsertWithDifferentSecondary() {
        List<Object[]> inputs = new ArrayList<>();
        inputs.add(new Object[] { 1, false, 2, 1 });
        inputs.add(new Object[] { 0, false, 1, 1 });

        List<Object[]> outputs = new ArrayList<>();
        outputs.add(new Object[] { 1, false, 2, 1 });
        outputs.add(new Object[] { 0, true, 2, 1 });
        outputs.add(new Object[] { 0, false, 1, 1 });

        List<Object[]> buddyOutputs = new ArrayList<>();
        buddyOutputs.add(new Object[] { 1, false, 2, 1 });
        buddyOutputs.add(new Object[] { 0, true, null, 1 });
        buddyOutputs.add(new Object[] { 0, false, 1, 1 });
        try {
            runTest(inputs, outputs, new int[] { 1, 0 }, false);
            runTest(inputs, buddyOutputs, new int[] { 1, 0 }, true);
        } catch (HyracksDataException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDelete() {
        List<Object[]> inputs = new ArrayList<>();
        inputs.add(new Object[] { 2, false, 1, 1 });
        inputs.add(new Object[] { 1, true, 1, 1 });
        inputs.add(new Object[] { 0, false, 2, 1 });

        List<Object[]> outputs = new ArrayList<>();
        outputs.add(new Object[] { 2, false, 1, 1 });
        outputs.add(new Object[] { 1, true, 1, 1 });
        outputs.add(new Object[] { 0, false, 2, 1 });

        List<Object[]> buddyOutputs = new ArrayList<>();
        buddyOutputs.add(new Object[] { 2, false, 1, 1 });
        buddyOutputs.add(new Object[] { 1, true, null, 1 });
        buddyOutputs.add(new Object[] { 0, false, 2, 1 });

        try {
            runTest(inputs, outputs, new int[] { 0, 1, 0 }, false);
            runTest(inputs, buddyOutputs, new int[] { 0, 1, 0 }, true);

        } catch (HyracksDataException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDeleteNullSecondary() {
        List<Object[]> inputs = new ArrayList<>();
        inputs.add(new Object[] { 1, false, null, 1 });
        inputs.add(new Object[] { 0, true, null, 1 });

        List<Object[]> outputs = new ArrayList<>();

        List<Object[]> buddyOutputs = new ArrayList<>();

        try {
            runTest(inputs, outputs, new int[] { 0, 0 }, false);
            runTest(inputs, buddyOutputs, new int[] { 0, 0 }, true);
        } catch (HyracksDataException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNullSecondary() {
        List<Object[]> inputs = new ArrayList<>();
        inputs.add(new Object[] { 2, false, null, 1 });
        inputs.add(new Object[] { 1, false, 2, 1 });
        inputs.add(new Object[] { 0, false, null, 1 });

        List<Object[]> outputs = new ArrayList<>();
        outputs.add(new Object[] { 1, false, 2, 1 });
        outputs.add(new Object[] { 0, true, 2, 1 });

        List<Object[]> buddyOutputs = new ArrayList<>();
        buddyOutputs.add(new Object[] { 1, false, 2, 1 });
        buddyOutputs.add(new Object[] { 0, true, null, 1 });
        try {
            runTest(inputs, outputs, new int[] { 1, 0, 0 }, false);
            runTest(inputs, buddyOutputs, new int[] { 1, 0, 0 }, true);
        } catch (HyracksDataException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void runTest(List<Object[]> inputTuples, List<Object[]> expectedTuples, int[] numDeletedTuples,
            boolean hasBuddyBTree) throws HyracksDataException {
        List<Object> stateObjects = new ArrayList<>();
        IHyracksJobletContext jobletContext = Mockito.mock(IHyracksJobletContext.class);
        IHyracksTaskContext ctx = Mockito.mock(IHyracksTaskContext.class);
        Mockito.when(ctx.getJobletContext()).thenReturn(jobletContext);
        Mockito.when(ctx.getInitialFrameSize()).thenReturn(FRAME_SIZE);
        Mockito.when(ctx.allocateFrame(FRAME_SIZE)).thenAnswer(new Answer<ByteBuffer>() {
            @Override
            public ByteBuffer answer(InvocationOnMock invocation) throws Throwable {
                return ByteBuffer.allocate(FRAME_SIZE);
            }
        });

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                stateObjects.add(invocation.getArguments()[0]);
                return null;
            }
        }).when(ctx).setStateObject(Mockito.any(IStateObject.class));

        List<ITupleReference> resultTuples = new ArrayList<>();
        IFrameWriter resultWriter = new ResultFrameWriter(resultTuples);

        LSMSecondaryIndexCreationTupleProcessorNodePushable op =
                new LSMSecondaryIndexCreationTupleProcessorNodePushable(ctx, 0, recDesc, MissingWriterFactory.INSTANCE,
                        numTagFields, numSecondaryKeys, numPrimaryKeys, hasBuddyBTree);
        op.setOutputFrameWriter(0, resultWriter, recDesc);

        op.open();

        IFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender(frame, true);
        // generate input tuples
        for (Object[] inputTuple : inputTuples) {
            ITupleReference tuple = buildTuple(inputTuple);
            appender.append(tuple);
        }
        appender.write(op, true);
        op.close();

        // check results
        Assert.assertEquals("Check the number of returned tuples", expectedTuples.size(), resultTuples.size());

        for (int i = 0; i < expectedTuples.size(); i++) {
            Object[] expectedTuple = expectedTuples.get(i);
            ITupleReference resultTuple = resultTuples.get(i);

            // check component pos
            Assert.assertEquals("Check component position", expectedTuple[0],
                    IntegerPointable.getInteger(resultTuple.getFieldData(0), resultTuple.getFieldStart(0)));

            // check anti-matter flag
            Assert.assertEquals("Check anti-matter", expectedTuple[1],
                    BooleanPointable.getBoolean(resultTuple.getFieldData(1), resultTuple.getFieldStart(1)));

            if (expectedTuple[2] == null) {
                Assert.assertTrue("Check secondary key is empty",
                        TypeTagUtil.isType(resultTuple, 2, ATypeTag.SERIALIZED_MISSING_TYPE_TAG));
            } else {
                Assert.assertEquals("Check secondary key", expectedTuple[2],
                        IntegerPointable.getInteger(resultTuple.getFieldData(2), resultTuple.getFieldStart(2)));
            }

            Assert.assertEquals("Check primary key", expectedTuple[3],
                    IntegerPointable.getInteger(resultTuple.getFieldData(3), resultTuple.getFieldStart(3)));
        }

        //check num deleted tuples
        Assert.assertEquals("Check state object", 1, stateObjects.size());
        DeletedTupleCounter counter = (DeletedTupleCounter) stateObjects.get(0);
        for (int i = 0; i < numDeletedTuples.length; i++) {
            Assert.assertEquals("Check num of deleted tuples", numDeletedTuples[i], counter.get(i));
        }
    }

    private ITupleReference buildTuple(Object[] values) throws HyracksDataException {
        ArrayTupleBuilder builder = new ArrayTupleBuilder(numTagFields + numSecondaryKeys + numPrimaryKeys);
        builder.addField(IntegerSerializerDeserializer.INSTANCE, (int) values[0]);
        builder.addField(BooleanSerializerDeserializer.INSTANCE, (boolean) values[1]);
        if (values[2] == null) {
            IMissingWriter writer = MissingWriterFactory.INSTANCE.createMissingWriter();
            writer.writeMissing(builder.getDataOutput());
            builder.addFieldEndOffset();
        } else {
            builder.addField(IntegerSerializerDeserializer.INSTANCE, (int) values[2]);
        }

        builder.addField(IntegerSerializerDeserializer.INSTANCE, (int) values[3]);
        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(builder.getFieldEndOffsets(), builder.getByteArray());
        return tuple;
    }
}
