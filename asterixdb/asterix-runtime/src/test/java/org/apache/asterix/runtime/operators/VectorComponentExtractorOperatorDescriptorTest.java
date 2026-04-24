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

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.NoOpWarningCollector;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link VectorComponentExtractorOperatorDescriptor}: flattening a serialized ADM
 * ordered list of numbers into one single-field ADouble tuple per component.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class VectorComponentExtractorOperatorDescriptorTest {

    private static final int FRAME_SIZE = 32768;

    private final RecordDescriptor inRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANY) });

    private final RecordDescriptor outRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE) });

    private static class ResultFrameWriter implements IFrameWriter {
        private final FrameTupleAccessor resultAccessor;
        private final FrameTupleReference tuple = new FrameTupleReference();
        private final List<ITupleReference> resultTuples;

        ResultFrameWriter(RecordDescriptor recDesc, List<ITupleReference> resultTuples) {
            this.resultAccessor = new FrameTupleAccessor(recDesc);
            this.resultTuples = resultTuples;
        }

        @Override
        public void open() {
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
        public void fail() {
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testDoubleListEmitsOneTuplePerComponent() throws Exception {
        List<byte[]> inputs = new ArrayList<>();
        inputs.add(buildDoubleList(1.5, 2.5, 3.5));
        double[] result = runOperator(inputs);
        Assert.assertArrayEquals(new double[] { 1.5, 2.5, 3.5 }, result, 0.0);
    }

    @Test
    public void testNullAndMissingFieldEmitNothing() throws Exception {
        List<byte[]> inputs = new ArrayList<>();
        inputs.add(new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG });
        inputs.add(new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG });
        double[] result = runOperator(inputs);
        Assert.assertEquals(0, result.length);
    }

    @Test
    public void testNonListFieldEmitsNothing() throws Exception {
        List<byte[]> inputs = new ArrayList<>();
        inputs.add(buildTaggedDouble(42.0));
        double[] result = runOperator(inputs);
        Assert.assertEquals(0, result.length);
    }

    @Test
    public void testHeterogeneousListCoercesToDouble() throws Exception {
        List<byte[]> inputs = new ArrayList<>();
        inputs.add(buildAnyList(new Object[] { 2, 3.5, 7 }));
        double[] result = runOperator(inputs);
        Assert.assertArrayEquals(new double[] { 2.0, 3.5, 7.0 }, result, 0.0);
    }

    @Test
    public void testMultipleInputTuplesConcatenateOutputs() throws Exception {
        List<byte[]> inputs = new ArrayList<>();
        inputs.add(buildDoubleList(1.0, 2.0));
        inputs.add(new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG });
        inputs.add(buildDoubleList(3.0));
        double[] result = runOperator(inputs);
        Assert.assertArrayEquals(new double[] { 1.0, 2.0, 3.0 }, result, 0.0);
    }

    /**
     * Runs the operator over a single input frame containing one tuple per given field-0 value and
     * returns the emitted double components in order.
     */
    private double[] runOperator(List<byte[]> vectorFields) throws HyracksDataException {
        IHyracksTaskContext ctx = mockTaskContext();
        JobSpecification spec = new JobSpecification();
        VectorComponentExtractorOperatorDescriptor desc = new VectorComponentExtractorOperatorDescriptor(spec,
                new ColumnAccessEvalFactory(0), inRecDesc, outRecDesc);

        IRecordDescriptorProvider rdp = Mockito.mock(IRecordDescriptorProvider.class);
        Mockito.when(rdp.getInputRecordDescriptor(Mockito.any(), Mockito.anyInt())).thenReturn(inRecDesc);

        List<ITupleReference> resultTuples = new ArrayList<>();
        IOperatorNodePushable op = desc.createPushRuntime(ctx, rdp, 0, 1);
        op.setOutputFrameWriter(0, new ResultFrameWriter(outRecDesc, resultTuples), outRecDesc);

        IFrameWriter input = op.getInputFrameWriter(0);
        input.open();
        IFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender(frame, true);
        for (byte[] field : vectorFields) {
            ArrayTupleBuilder builder = new ArrayTupleBuilder(1);
            builder.addField(field, 0, field.length);
            if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                Assert.fail("Test input frame overflow");
            }
        }
        appender.write(input, true);
        input.close();

        double[] values = new double[resultTuples.size()];
        for (int i = 0; i < resultTuples.size(); i++) {
            ITupleReference t = resultTuples.get(i);
            Assert.assertEquals("Output tuple must have a single field", 1, t.getFieldCount());
            byte[] data = t.getFieldData(0);
            int start = t.getFieldStart(0);
            Assert.assertEquals("Output field must be a tagged ADouble", ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG,
                    data[start]);
            values[i] = ADoubleSerializerDeserializer.getDouble(data, start + 1);
        }
        return values;
    }

    private IHyracksTaskContext mockTaskContext() throws HyracksDataException {
        IHyracksJobletContext jobletContext = Mockito.mock(IHyracksJobletContext.class);
        Mockito.when(jobletContext.getServiceContext()).thenReturn(Mockito.mock(INCServiceContext.class));
        IHyracksTaskContext ctx = Mockito.mock(IHyracksTaskContext.class);
        Mockito.when(ctx.getJobletContext()).thenReturn(jobletContext);
        Mockito.when(ctx.getWarningCollector()).thenReturn(NoOpWarningCollector.INSTANCE);
        Mockito.when(ctx.getInitialFrameSize()).thenReturn(FRAME_SIZE);
        Mockito.when(ctx.allocateFrame(Mockito.anyInt()))
                .thenAnswer(invocation -> ByteBuffer.allocate((int) invocation.getArguments()[0]));
        return ctx;
    }

    /** Serialized ADM ordered list of doubles (tagged), matching the AOrderedListType(ADOUBLE) format. */
    private static byte[] buildDoubleList(double... values) throws HyracksDataException {
        try {
            OrderedListBuilder listBuilder = new OrderedListBuilder();
            listBuilder.reset(new AOrderedListType(BuiltinType.ADOUBLE, null));
            ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
            AMutableDouble aDouble = new AMutableDouble(0.0);
            for (double v : values) {
                itemStorage.reset();
                aDouble.setValue(v);
                itemStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, itemStorage.getDataOutput());
                listBuilder.addItem(itemStorage);
            }
            ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
            listBuilder.write(listStorage.getDataOutput(), true);
            byte[] result = new byte[listStorage.getLength()];
            System.arraycopy(listStorage.getByteArray(), listStorage.getStartOffset(), result, 0, result.length);
            return result;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Serialized heterogeneous (item type ANY) ordered list of Integer/Double values. */
    @SuppressWarnings("unchecked")
    private static byte[] buildAnyList(Object[] values) throws HyracksDataException {
        try {
            OrderedListBuilder listBuilder = new OrderedListBuilder();
            listBuilder.reset(new AOrderedListType(BuiltinType.ANY, null));
            ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
            ISerializerDeserializer<Object> intSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
            ISerializerDeserializer<Object> doubleSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
            AMutableInt32 aInt = new AMutableInt32(0);
            AMutableDouble aDouble = new AMutableDouble(0.0);
            for (Object v : values) {
                itemStorage.reset();
                if (v instanceof Integer) {
                    aInt.setValue((Integer) v);
                    intSerde.serialize(aInt, itemStorage.getDataOutput());
                } else {
                    aDouble.setValue((Double) v);
                    doubleSerde.serialize(aDouble, itemStorage.getDataOutput());
                }
                listBuilder.addItem(itemStorage);
            }
            ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
            listBuilder.write(listStorage.getDataOutput(), true);
            byte[] result = new byte[listStorage.getLength()];
            System.arraycopy(listStorage.getByteArray(), listStorage.getStartOffset(), result, 0, result.length);
            return result;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private static byte[] buildTaggedDouble(double value) throws HyracksDataException {
        try {
            ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
            storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
            ADoubleSerializerDeserializer.INSTANCE.serialize(new AMutableDouble(value), storage.getDataOutput());
            byte[] result = new byte[storage.getLength()];
            System.arraycopy(storage.getByteArray(), storage.getStartOffset(), result, 0, result.length);
            return result;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
