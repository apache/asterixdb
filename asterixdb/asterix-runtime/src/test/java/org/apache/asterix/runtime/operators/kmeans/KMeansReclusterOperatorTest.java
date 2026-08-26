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

package org.apache.asterix.runtime.operators.kmeans;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorListDecoder;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct-drive test of the CLUSTER BY k-means|| RECLUSTER stage
 * ({@link KMeansReclusterOperatorDescriptor}): feed a scrambled envelope partials stream and assert the merge —
 * partials accumulated per pool member regardless of arrival order, and the weighted means reduced to at
 * most {@code count} centroids.
 * <p>
 * Activity order follows {@link KMeansReclusterOperatorDescriptor#contributeActivities}: StorePool (0),
 * Score (1).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class KMeansReclusterOperatorTest {
    // Same value as SqlppClusterByVisitor's RECLUSTER_SEED_DEFAULT, the seed used when a query sets none
    // (mirrored here: this module cannot see that class). The expected picks below depend on it.
    private static final long DEFAULT_RECLUSTER_SEED = 12345L;

    /** Enough frames for the partial sort to run in memory in this fixture. */
    private static final int TEST_FRAMES_LIMIT = 32;

    @SuppressWarnings("rawtypes")
    private static final RecordDescriptor VEC_REC_DESC =
            new RecordDescriptor(new ISerializerDeserializer[] { ByteArraySerializerDeserializer.INSTANCE });

    @Test
    public void reclusterMergesPartials() throws Exception {
        // Two candidates attracted rows and k is two, so the reduction is satisfied and both centres come out.
        List<double[]> out = runRecluster(2);

        // idx 0 -> weight 3, centre (1,2) (its two partials accumulate across the scrambled arrival order);
        // idx 1 -> weight 3, centre (8,9). Their relative order is not asserted: the reduction draws among
        // equally weighted candidates, so pinning an order here would only pin the generator's seed.
        Assert.assertEquals(2, out.size());
        Assert.assertTrue("expected the centre (1,2) among the emitted centroids",
                out.stream().anyMatch(v -> Arrays.equals(v, new double[] { 1, 2 })));
        Assert.assertTrue("expected the centre (8,9) among the emitted centroids",
                out.stream().anyMatch(v -> Arrays.equals(v, new double[] { 8, 9 })));
    }

    @Test
    public void reclusterEmitsWhatItHasWhenShortOfK() throws Exception {
        // Same input, k of three. Candidate 2 attracted nothing, and a candidate attracts nothing only when
        // an earlier one sits at the same point -- so there is no third position to cluster around. Asking
        // for more clusters than the data can separate is a question the data answers, so the two that exist
        // come back and the shortfall is a warning, not a failure.
        List<double[]> out = runRecluster(3);

        Assert.assertEquals("the groups that exist must still be returned", 2, out.size());
        Assert.assertTrue("expected the centre (1,2) among the emitted centroids",
                out.stream().anyMatch(v -> Arrays.equals(v, new double[] { 1, 2 })));
        Assert.assertTrue("expected the centre (8,9) among the emitted centroids",
                out.stream().anyMatch(v -> Arrays.equals(v, new double[] { 8, 9 })));
    }

    /** Three candidates, only two of which attracted rows, reduced to {@code k}. */
    private static List<double[]> runRecluster(int k) throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(32768);
        JobSpecification spec = new JobSpecification();
        KMeansReclusterOperatorDescriptor op = new KMeansReclusterOperatorDescriptor(spec, VEC_REC_DESC, k, 0,
                TEST_FRAMES_LIMIT, DEFAULT_RECLUSTER_SEED);

        // Single-input merge — activities: StorePool (0), Score (1). No vector input.
        List<IActivity> activities = collectActivities(op);
        IRecordDescriptorProvider rdp = recordDescProvider();

        IOperatorNodePushable poolStore = activities.get(0).createPushRuntime(ctx, rdp, 0, 1);
        poolStore.getInputFrameWriter(0).open();
        poolStore.getInputFrameWriter(0)
                .nextFrame(envelopesFrame(ctx,
                        new double[][] {
                                // kind, partition, seq, score, v0, v1 — scrambled arrival on purpose
                                { 2, 1, 0, 1, 1, 2 }, // partial: idx 0, count 1, sum (1,2)
                                { 0, 0, 1, 0, 8, 8 }, // pool member 1 = (8,8)
                                { 2, 1, 1, 3, 24, 27 }, // partial: idx 1, count 3, sum (24,27)
                                { 0, 0, 2, 0, 5, 5 }, // pool member 2 = (5,5) — no partial, so no centre
                                { 0, 0, 0, 0, 0, 0 }, // pool member 0 = (0,0)
                                { 2, 0, 0, 2, 2, 4 } })); // partial: idx 0, count 2, sum (2,4)
        poolStore.getInputFrameWriter(0).close();

        IOperatorNodePushable score = activities.get(1).createPushRuntime(ctx, rdp, 0, 1);
        return collectOutput(score, false);
    }

    private static List<IActivity> collectActivities(KMeansReclusterOperatorDescriptor op) {
        List<IActivity> activities = new ArrayList<>();
        op.contributeActivities(new IActivityGraphBuilder() {
            @Override
            public void addActivity(IOperatorDescriptor o, IActivity task) {
                activities.add(task);
            }

            @Override
            public void addBlockingEdge(IActivity blocker, IActivity blocked) {
            }

            @Override
            public void addSourceEdge(int operatorInputIndex, IActivity task, int taskInputIndex) {
            }

            @Override
            public void addTargetEdge(int operatorOutputIndex, IActivity task, int taskOutputIndex) {
            }
        });
        return activities;
    }

    private static IRecordDescriptorProvider recordDescProvider() {
        return new IRecordDescriptorProvider() {
            @Override
            public RecordDescriptor getInputRecordDescriptor(ActivityId aid, int inputIndex) {
                return VEC_REC_DESC;
            }

            @Override
            public RecordDescriptor getOutputRecordDescriptor(ActivityId aid, int outputIndex) {
                return VEC_REC_DESC;
            }
        };
    }

    /** Runs the Score pushable and decodes every output row (envelope rows flattened when asked). */
    private static List<double[]> collectOutput(IOperatorNodePushable score, boolean envelopes) throws Exception {
        List<double[]> out = new ArrayList<>();
        score.setOutputFrameWriter(0, new IFrameWriter() {
            private final FrameTupleAccessor fta = new FrameTupleAccessor(VEC_REC_DESC);

            @Override
            public void open() {
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                fta.reset(buffer);
                for (int i = 0; i < fta.getTupleCount(); i++) {
                    FrameTupleReference ref = new FrameTupleReference();
                    ref.reset(fta, i);
                    out.add(envelopes ? decodeEnvelope(ref) : decodeVector(ref));
                }
            }

            @Override
            public void fail() {
            }

            @Override
            public void close() {
            }
        }, VEC_REC_DESC);
        score.initialize();
        return out;
    }

    private static double[] decodeVector(FrameTupleReference tuple) throws HyracksDataException {
        try {
            VoidPointable p = new VoidPointable();
            p.set(tuple.getFieldData(0), tuple.getFieldStart(0), tuple.getFieldLength(0));
            ListAccessor la = new ListAccessor();
            la.reset(p.getByteArray(), p.getStartOffset());
            return new VectorListDecoder().createArrayFromList(la, new double[la.size()]);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Flattens an envelope row to [kind, partition, seq, score, vector...]. */
    private static double[] decodeEnvelope(FrameTupleReference tuple) throws HyracksDataException {
        try {
            VoidPointable p = new VoidPointable();
            p.set(tuple.getFieldData(0), tuple.getFieldStart(0), tuple.getFieldLength(0));
            ListAccessor la = new ListAccessor();
            la.reset(p.getByteArray(), p.getStartOffset());
            byte[] bytes = la.getByteArray();
            ListAccessor vecAccessor = new ListAccessor();
            vecAccessor.reset(bytes, la.getItemOffset(4));
            double[] vec = new VectorListDecoder().createArrayFromList(vecAccessor, new double[vecAccessor.size()]);
            double[] flat = new double[4 + vec.length];
            for (int i = 0; i < 4; i++) {
                flat[i] = org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer
                        .getDouble(bytes, la.getItemOffset(i) + 1);
            }
            System.arraycopy(vec, 0, flat, 4, vec.length);
            return flat;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * The fold must not depend on whether the partial sort spilled.
     * <p>
     * RECLUSTER streams its weigh partials through an external sort so they are never all resident. Two things
     * could go wrong and neither shows up at a single budget: the sorted order could differ once runs are
     * merged (floating-point addition is not associative, so a different order gives different centroids), and
     * the in-memory path is a genuinely different code path -- when nothing spills the generator sorts in place
     * and produces no runs at all, so a fold that only merged the run list would quietly return nothing.
     * <p>
     * So: same partials, one budget that forces spilling and one that does not, and the results must be equal
     * bit for bit.
     */
    @Test
    public void reclusterFoldIsIdenticalWhetherOrNotTheSortSpills() throws Exception {
        // Enough partials to exceed a 4-frame budget (3 usable x 32 KB) but sit comfortably in a large one.
        final int members = 600;
        final int partitionsPerMember = 5;
        double[][] rows = new double[members + members * partitionsPerMember][];
        int r = 0;
        for (int m = 0; m < members; m++) {
            rows[r++] = new double[] { 0, 0, m, 0, m, m + 1 }; // pool member m
        }
        // Partials arrive scrambled across partitions, as a concurrent M-to-1 connector would deliver them.
        for (int p = partitionsPerMember - 1; p >= 0; p--) {
            for (int m = members - 1; m >= 0; m--) {
                rows[r++] = new double[] { 2, p, m, 1, 0.1d * (p + 1), 1.0e8d * (p + 1) };
            }
        }
        List<double[]> spilled = runRecluster(rows, 4);
        List<double[]> inMemory = runRecluster(rows, 4096);

        Assert.assertEquals("centroid count differed between budgets", inMemory.size(), spilled.size());
        Assert.assertFalse("fixture produced no centroids; the test would prove nothing", spilled.isEmpty());
        for (int i = 0; i < spilled.size(); i++) {
            double[] a = spilled.get(i);
            double[] b = inMemory.get(i);
            Assert.assertEquals("centroid " + i + " width", b.length, a.length);
            for (int d = 0; d < a.length; d++) {
                Assert.assertEquals("centroid " + i + " component " + d + " differed with the frame budget",
                        Double.doubleToLongBits(b[d]), Double.doubleToLongBits(a[d]));
            }
        }
    }

    /** Drives RECLUSTER over the given envelope rows at a chosen frame budget. */
    private static List<double[]> runRecluster(double[][] rows, int framesLimit) throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(32768);
        JobSpecification spec = new JobSpecification();
        KMeansReclusterOperatorDescriptor op =
                new KMeansReclusterOperatorDescriptor(spec, VEC_REC_DESC, 8, 0, framesLimit, DEFAULT_RECLUSTER_SEED);
        List<IActivity> activities = collectActivities(op);
        IRecordDescriptorProvider rdp = recordDescProvider();
        IOperatorNodePushable poolStore = activities.get(0).createPushRuntime(ctx, rdp, 0, 1);
        poolStore.getInputFrameWriter(0).open();
        // Fed in batches: one VSizeFrame holds a bounded number of envelopes, and the point of the fixture is
        // to be larger than a frame -- that is what makes the small budget spill.
        final int perFrame = 100;
        for (int from = 0; from < rows.length; from += perFrame) {
            int to = Math.min(from + perFrame, rows.length);
            double[][] batch = new double[to - from][];
            System.arraycopy(rows, from, batch, 0, to - from);
            poolStore.getInputFrameWriter(0).nextFrame(envelopesFrame(ctx, batch));
        }
        poolStore.getInputFrameWriter(0).close();
        IOperatorNodePushable score = activities.get(1).createPushRuntime(ctx, rdp, 0, 1);
        return collectOutput(score, false);
    }

    /** Builds a frame of envelope rows, each given as [kind, partition, seq, score, v...]. */
    private static ByteBuffer envelopesFrame(IHyracksTaskContext ctx, double[][] rows) throws HyracksDataException {
        try {
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender appender = new FrameTupleAppender(frame);
            ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
            OrderedListBuilder lb = new OrderedListBuilder();
            OrderedListBuilder vb = new OrderedListBuilder();
            ArrayBackedValueStorage item = new ArrayBackedValueStorage();
            ArrayBackedValueStorage vecStorage = new ArrayBackedValueStorage();
            AOrderedListType openList = new AOrderedListType(BuiltinType.ANY, null);
            for (double[] row : rows) {
                tb.reset();
                lb.reset(openList);
                for (int i = 0; i < 4; i++) {
                    item.reset();
                    item.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    item.getDataOutput().writeDouble(row[i]);
                    lb.addItem(item);
                }
                vb.reset(openList);
                for (int i = 4; i < row.length; i++) {
                    item.reset();
                    item.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    item.getDataOutput().writeDouble(row[i]);
                    vb.addItem(item);
                }
                vecStorage.reset();
                vb.write(vecStorage.getDataOutput(), true);
                lb.addItem(vecStorage);
                lb.write(tb.getDataOutput(), true);
                tb.addFieldEndOffset();
                Assert.assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
            }
            return frame.getBuffer();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private static ByteBuffer vectorsFrame(IHyracksTaskContext ctx, double[][] vectors) throws HyracksDataException {
        try {
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender appender = new FrameTupleAppender(frame);
            ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
            OrderedListBuilder lb = new OrderedListBuilder();
            ArrayBackedValueStorage item = new ArrayBackedValueStorage();
            AOrderedListType listType = new AOrderedListType(BuiltinType.ADOUBLE, null);
            for (double[] vec : vectors) {
                tb.reset();
                lb.reset(listType);
                for (double d : vec) {
                    item.reset();
                    item.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    item.getDataOutput().writeDouble(d);
                    lb.addItem(item);
                }
                lb.write(tb.getDataOutput(), true);
                tb.addFieldEndOffset();
                Assert.assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
            }
            return frame.getBuffer();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    static {
        Arrays.hashCode(new int[0]); // keep Arrays import used across JDK format variations
    }
}
