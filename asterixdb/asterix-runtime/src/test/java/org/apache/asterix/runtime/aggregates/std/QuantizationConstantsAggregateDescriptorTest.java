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
package org.apache.asterix.runtime.aggregates.std;

import java.util.Arrays;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.aggregates.serializable.std.BufferSerDeUtil;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.NoOpWarningCollector;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for the aggregate function created by {@link QuantizationConstantsAggregateDescriptor}.
 * The local step collects numeric values into a BINARY blob of layout [count:int][doubles...]; the
 * global step consumes such blobs and emits a BINARY blob with the serialized quantization constants
 * [minQ:float][maxQ:float][alpha:float][bits:int][confidence:float][sampleCount:int].
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class QuantizationConstantsAggregateDescriptorTest {

    /** Parsed form of the global-step output blob. */
    private static class Constants {
        final float minQ;
        final float maxQ;
        final float alpha;
        final int bits;
        final float confidence;
        final int sampleCount;

        Constants(float minQ, float maxQ, float alpha, int bits, float confidence, int sampleCount) {
            this.minQ = minQ;
            this.maxQ = maxQ;
            this.alpha = alpha;
            this.bits = bits;
            this.confidence = confidence;
            this.sampleCount = sampleCount;
        }
    }

    /** Minimal single-tuple {@link IFrameTupleReference} over pre-serialized field bytes. */
    private static class SimpleTuple implements IFrameTupleReference {
        private final byte[][] fields;

        SimpleTuple(byte[]... fields) {
            this.fields = fields;
        }

        @Override
        public int getFieldCount() {
            return fields.length;
        }

        @Override
        public byte[] getFieldData(int fIdx) {
            return fields[fIdx];
        }

        @Override
        public int getFieldStart(int fIdx) {
            return 0;
        }

        @Override
        public int getFieldLength(int fIdx) {
            return fields[fIdx].length;
        }

        @Override
        public FrameTupleAccessor getFrameTupleAccessor() {
            return null;
        }

        @Override
        public int getTupleIndex() {
            return 0;
        }
    }

    @Test
    public void testLocalStepEmitsCountPrefixedDoubleBlob() throws Exception {
        double[] values = { 3.25, -1.5, 2.75 };
        byte[] blob = runLocal(0.9f, 8, values);
        double[] parsed = parseLocalBlob(blob);
        // The local step preserves input order (no sorting happens until the global step).
        Assert.assertArrayEquals(values, parsed, 0.0);
    }

    @Test
    public void testLocalStepSingleValue() throws Exception {
        byte[] blob = runLocal(0.9f, 8, new double[] { 7.5 });
        double[] parsed = parseLocalBlob(blob);
        Assert.assertArrayEquals(new double[] { 7.5 }, parsed, 0.0);
    }

    @Test
    public void testGlobalStepComputesQuantilesAndAlpha() throws Exception {
        // Values 1.0 .. 100.0 fed to the global step through a local blob.
        double[] values = new double[100];
        for (int i = 0; i < 100; i++) {
            values[i] = i + 1.0;
        }
        float confidence = 0.9f;
        int bits = 8;
        byte[] localBlob = runLocal(confidence, bits, values);
        Constants constants = parseConstantsBlob(runGlobal(confidence, bits, localBlob));

        // Hand-computed expectations, mirroring the float arithmetic in finish():
        // half = (1 - 0.9) / 2 = 0.05; lowerIdx = floor(0.05 * 99) = 4 -> sorted[4] = 5.0
        // upperIdx = ceil(0.95 * 99) = 95 -> sorted[95] = 96.0; alpha = 255 / (96 - 5)
        Assert.assertEquals(5.0f, constants.minQ, 0.0f);
        Assert.assertEquals(96.0f, constants.maxQ, 0.0f);
        Assert.assertEquals(255.0f / 91.0f, constants.alpha, 1e-6f);
        Assert.assertEquals(bits, constants.bits);
        Assert.assertEquals(confidence, constants.confidence, 0.0f);
        Assert.assertEquals(100, constants.sampleCount);
    }

    @Test
    public void testGlobalStepMergesMultipleLocalBlobs() throws Exception {
        double[] first = new double[50];
        double[] second = new double[50];
        for (int i = 0; i < 50; i++) {
            first[i] = i + 1.0; // 1..50
            second[i] = i + 51.0; // 51..100
        }
        float confidence = 0.9f;
        int bits = 8;
        byte[] blob1 = runLocal(confidence, bits, first);
        byte[] blob2 = runLocal(confidence, bits, second);
        Constants constants = parseConstantsBlob(runGlobal(confidence, bits, blob1, blob2));
        // Same distribution as testGlobalStepComputesQuantilesAndAlpha, just split into two blobs.
        Assert.assertEquals(5.0f, constants.minQ, 0.0f);
        Assert.assertEquals(96.0f, constants.maxQ, 0.0f);
        Assert.assertEquals(255.0f / 91.0f, constants.alpha, 1e-6f);
        Assert.assertEquals(100, constants.sampleCount);
    }

    @Test
    public void testGlobalStepSingleValue() throws Exception {
        // A single sample: minQ == maxQ triggers the div-by-zero guard which widens maxQ by 1e-6f,
        // yielding a very large but finite positive alpha.
        byte[] localBlob = runLocal(0.9f, 8, new double[] { 7.5 });
        Constants constants = parseConstantsBlob(runGlobal(0.9f, 8, localBlob));
        Assert.assertEquals(7.5f, constants.minQ, 0.0f);
        Assert.assertTrue("Guard must widen maxQ above minQ", constants.maxQ > constants.minQ);
        Assert.assertEquals(7.5f + 1e-6f, constants.maxQ, 0.0f);
        Assert.assertTrue("Alpha must be finite", Float.isFinite(constants.alpha));
        Assert.assertTrue("Alpha must be positive", constants.alpha > 0.0f);
        Assert.assertEquals(1, constants.sampleCount);
    }

    @Test
    public void testGlobalStepAllEqualValues() throws Exception {
        double[] values = new double[10];
        Arrays.fill(values, 4.25);
        byte[] localBlob = runLocal(0.9f, 8, values);
        Constants constants = parseConstantsBlob(runGlobal(0.9f, 8, localBlob));
        Assert.assertEquals(4.25f, constants.minQ, 0.0f);
        Assert.assertTrue("Guard must widen maxQ above minQ", constants.maxQ > constants.minQ);
        Assert.assertTrue("Alpha must be finite", Float.isFinite(constants.alpha));
        Assert.assertTrue("Alpha must be positive", constants.alpha > 0.0f);
        Assert.assertEquals(10, constants.sampleCount);
    }

    @Test
    public void testEmptyAggregateEmitsSystemNull() throws Exception {
        IAggregateEvaluator agg = createEvaluator(0.9f, 8);
        agg.init();
        IPointable result = new VoidPointable();
        agg.finish(result);
        Assert.assertEquals(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG, result.getByteArray()[result.getStartOffset()]);
    }

    @Test
    public void testStepSkipsNullAndMissing() throws Exception {
        IAggregateEvaluator agg = createEvaluator(0.9f, 8);
        agg.init();
        agg.step(new SimpleTuple(new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG }));
        agg.step(new SimpleTuple(new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG }));
        agg.step(new SimpleTuple(taggedDouble(1.25)));
        IPointable result = new VoidPointable();
        agg.finish(result);
        double[] parsed = parseLocalBlob(copyPointable(result));
        Assert.assertArrayEquals(new double[] { 1.25 }, parsed, 0.0);
    }

    private IAggregateEvaluator createEvaluator(float confidence, int bits) throws HyracksDataException {
        QuantizationConstantsAggregateDescriptor descriptor =
                (QuantizationConstantsAggregateDescriptor) QuantizationConstantsAggregateDescriptor.FACTORY
                        .createFunctionDescriptor();
        descriptor.setImmutableStates(confidence, bits);
        IEvaluatorContext evalCtx = Mockito.mock(IEvaluatorContext.class);
        Mockito.when(evalCtx.getWarningCollector()).thenReturn(NoOpWarningCollector.INSTANCE);
        return descriptor
                .createAggregateEvaluatorFactory(new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0) })
                .createAggregateEvaluator(evalCtx);
    }

    /** Runs a local aggregation step over plain doubles and returns the serialized ABinary result field. */
    private byte[] runLocal(float confidence, int bits, double[] values) throws HyracksDataException {
        IAggregateEvaluator agg = createEvaluator(confidence, bits);
        agg.init();
        for (double v : values) {
            agg.step(new SimpleTuple(taggedDouble(v)));
        }
        IPointable result = new VoidPointable();
        agg.finish(result);
        return copyPointable(result);
    }

    /** Runs a global aggregation over previously produced local BINARY blobs. */
    private byte[] runGlobal(float confidence, int bits, byte[]... localBlobs) throws HyracksDataException {
        IAggregateEvaluator agg = createEvaluator(confidence, bits);
        agg.init();
        for (byte[] blob : localBlobs) {
            agg.step(new SimpleTuple(blob));
        }
        IPointable result = new VoidPointable();
        agg.finish(result);
        return copyPointable(result);
    }

    private static byte[] copyPointable(IPointable p) {
        byte[] copy = new byte[p.getLength()];
        System.arraycopy(p.getByteArray(), p.getStartOffset(), copy, 0, copy.length);
        return copy;
    }

    private static byte[] taggedDouble(double value) throws HyracksDataException {
        try {
            ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
            storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
            storage.getDataOutput().writeDouble(value);
            byte[] result = new byte[storage.getLength()];
            System.arraycopy(storage.getByteArray(), storage.getStartOffset(), result, 0, result.length);
            return result;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Extracts the binary content bytes (past the BINARY tag and length prefix) of a serialized ABinary. */
    private static ByteArrayPointable binaryContent(byte[] serialized) {
        Assert.assertEquals("Expected a BINARY-tagged result", ATypeTag.SERIALIZED_BINARY_TYPE_TAG, serialized[0]);
        ByteArrayPointable bap = new ByteArrayPointable();
        bap.set(serialized, 1, serialized.length - 1);
        return bap;
    }

    private static double[] parseLocalBlob(byte[] serialized) {
        ByteArrayPointable bap = binaryContent(serialized);
        byte[] bytes = bap.getByteArray();
        int offset = bap.getContentStartOffset();
        int count = IntegerPointable.getInteger(bytes, offset);
        Assert.assertEquals("Blob length must match [count:int][doubles...] layout",
                Integer.BYTES + count * Double.BYTES, bap.getContentLength());
        double[] values = new double[count];
        for (int i = 0; i < count; i++) {
            values[i] = BufferSerDeUtil.getDouble(bytes, offset + Integer.BYTES + i * Double.BYTES);
        }
        return values;
    }

    private static Constants parseConstantsBlob(byte[] serialized) {
        ByteArrayPointable bap = binaryContent(serialized);
        byte[] bytes = bap.getByteArray();
        int offset = bap.getContentStartOffset();
        Assert.assertEquals("Constants blob must be 3 floats + int + float + int", 24, bap.getContentLength());
        float minQ = FloatPointable.getFloat(bytes, offset);
        float maxQ = FloatPointable.getFloat(bytes, offset + 4);
        float alpha = FloatPointable.getFloat(bytes, offset + 8);
        int bits = IntegerPointable.getInteger(bytes, offset + 12);
        float confidence = FloatPointable.getFloat(bytes, offset + 16);
        int sampleCount = IntegerPointable.getInteger(bytes, offset + 20);
        return new Constants(minQ, maxQ, alpha, bits, confidence, sampleCount);
    }
}
