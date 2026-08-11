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

import static org.apache.asterix.om.types.BuiltinType.ADOUBLE;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.NoOpWarningCollector;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor}, driving both
 * activities (materializer sink + hierarchical clustering source) directly against a mocked task
 * context backed by a real {@link IOManager} for the materialized-sample run file.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class HierarchicalKMeansPlusPlusCentroidsOperatorDescriptorTest {

    private static final int DIM = 4;
    private static final AOrderedListType DOUBLE_LIST_TYPE = new AOrderedListType(BuiltinType.ADOUBLE, null);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /** Parsed output tuple: (treeLevel, centroidId, parentClusterId, embedding). */
    private static class CentroidTuple {
        final int treeLevel;
        final int centroidId;
        final int parentClusterId;
        final double[] embedding;

        CentroidTuple(int treeLevel, int centroidId, int parentClusterId, double[] embedding) {
            this.treeLevel = treeLevel;
            this.centroidId = centroidId;
            this.parentClusterId = parentClusterId;
            this.embedding = embedding;
        }
    }

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

    // ------------------------------------------------------------------ tests

    /**
     * Off-dimension vectors are never indexed, so they must not shape the centroids either. Before the
     * guard, a longer one met a shorter centroid in the distance loop and threw
     * {@link ArrayIndexOutOfBoundsException}.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    @Test
    public void testOffDimensionVectorsDoNotShapeCentroids() throws Exception {
        List<double[]> baseline = twoClusterVectors(20);
        List<CentroidTuple> expected = parseAll(runOperator(42L, 2, 32768, baseline));

        List<double[]> polluted = new ArrayList<>(baseline);
        for (int i = 0; i < 10; i++) {
            polluted.add(new double[DIM + 1]);
            polluted.add(new double[DIM - 1]);
        }
        List<CentroidTuple> actual = parseAll(runOperator(42L, 2, 32768, polluted));

        Assert.assertEquals("Off-dimension vectors must not change the centroid count", expected.size(), actual.size());
        for (CentroidTuple t : actual) {
            Assert.assertEquals("Centroids must carry the declared dimension", DIM, t.embedding.length);
        }
    }

    /**
     * k-means emits no centroids rather than failing: the first build job has already rejected a wholly
     * non-indexable sample, so this only arises when the operator is driven on its own.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    @Test
    public void testNoIndexableVectorEmitsNoCentroids() throws Exception {
        List<ITupleReference> run = runOperator(42L, 2, 32768, twoClusterVectors(20), DIM + 4);
        Assert.assertTrue("A partition with nothing indexable must emit no centroids", run.isEmpty());
    }

    @Test
    public void testDeterministicOutputWithSameSeed() throws Exception {
        List<double[]> vectors = twoClusterVectors(20);
        List<ITupleReference> run1 = runOperator(42L, 2, 32768, vectors);
        List<ITupleReference> run2 = runOperator(42L, 2, 32768, vectors);

        Assert.assertFalse("Operator must emit centroids", run1.isEmpty());
        Assert.assertEquals("Same seed must produce the same tuple count", run1.size(), run2.size());
        for (int i = 0; i < run1.size(); i++) {
            assertTuplesByteIdentical("Tuple " + i + " differs between identically seeded runs", run1.get(i),
                    run2.get(i));
        }
    }

    @Test
    public void testDifferentSeedCompletes() throws Exception {
        List<double[]> vectors = twoClusterVectors(20);
        List<ITupleReference> run = runOperator(43L, 2, 32768, vectors);
        Assert.assertFalse("Operator must emit centroids for any seed", run.isEmpty());
        for (ITupleReference t : run) {
            CentroidTuple c = parseTuple(t);
            Assert.assertEquals("Embedding dimension must be preserved", DIM, c.embedding.length);
        }
    }

    @Test
    public void testStructuralInvariants() throws Exception {
        // A small initial frame size (512) forces doesLevelFitInFrame to fail for the first parent
        // level (6 centroids for K=36), so at least two levels of the hierarchy are emitted.
        List<double[]> vectors = twoClusterVectors(40);
        List<CentroidTuple> tuples = parseAll(runOperator(7L, 36, 512, vectors));
        Assert.assertFalse(tuples.isEmpty());

        int maxTreeLevel = 0;
        for (CentroidTuple t : tuples) {
            maxTreeLevel = Math.max(maxTreeLevel, t.treeLevel);
        }
        Assert.assertTrue("Expected a multi-level hierarchy to be emitted", maxTreeLevel >= 1);

        // (1) Bottom-up emission: treeLevel is non-increasing over the emission order, i.e. the
        // deepest emitted level comes first and the root level (treeLevel 0) comes last.
        Assert.assertEquals("First emitted tuple must belong to the deepest emitted level", maxTreeLevel,
                tuples.get(0).treeLevel);
        Assert.assertEquals("Last emitted tuple must belong to the root level", 0,
                tuples.get(tuples.size() - 1).treeLevel);
        for (int i = 1; i < tuples.size(); i++) {
            Assert.assertTrue("Emission must be bottom-up (non-increasing treeLevel)",
                    tuples.get(i).treeLevel <= tuples.get(i - 1).treeLevel);
        }

        // Count tuples per tree level.
        Map<Integer, Integer> levelCounts = new HashMap<>();
        for (CentroidTuple t : tuples) {
            levelCounts.merge(t.treeLevel, 1, Integer::sum);
        }

        // (2) Root level: parentClusterId == -1 for every root tuple.
        for (CentroidTuple t : tuples) {
            if (t.treeLevel == 0) {
                Assert.assertEquals("Root tuples must have no parent", -1, t.parentClusterId);
            } else {
                int parentCount = levelCounts.getOrDefault(t.treeLevel - 1, 0);
                Assert.assertTrue("Parent cluster id must reference a centroid of the level above",
                        t.parentClusterId >= 0 && t.parentClusterId < parentCount);
            }
        }

        // (3) BFS-from-root centroid ids: root ids start at 0 and each deeper level continues the
        // contiguous id range of the level above, in emission order within the level.
        int expectedId = 0;
        for (int level = 0; level <= maxTreeLevel; level++) {
            for (CentroidTuple t : tuples) {
                if (t.treeLevel == level) {
                    Assert.assertEquals("Centroid ids must be contiguous in BFS-from-root order", expectedId,
                            t.centroidId);
                    expectedId++;
                }
            }
        }

        // (4) Embeddings are well-formed and clipped to the operator's bounds.
        for (CentroidTuple t : tuples) {
            Assert.assertEquals(DIM, t.embedding.length);
            for (double v : t.embedding) {
                Assert.assertTrue("Centroid values must be finite", Double.isFinite(v));
                Assert.assertTrue("Centroid values must be clipped to [-1000, 1000]", v >= -1000.0 && v <= 1000.0);
            }
        }
    }

    @Test
    public void testClusteringSanityTwoSeparatedGroups() throws Exception {
        List<double[]> vectors = twoClusterVectors(20);
        List<CentroidTuple> tuples = parseAll(runOperator(42L, 2, 32768, vectors));
        // K=2 keeps the structure single-level, so the two emitted centroids are the leaf centroids.
        Assert.assertEquals("K=2 over one partition must emit exactly two centroids", 2, tuples.size());

        double[] meanA = groupMean(vectors, 0.0);
        double[] meanB = groupMean(vectors, 10.0);
        boolean nearA = false;
        boolean nearB = false;
        for (CentroidTuple t : tuples) {
            if (euclidean(t.embedding, meanA) < 2.0) {
                nearA = true;
            }
            if (euclidean(t.embedding, meanB) < 2.0) {
                nearB = true;
            }
        }
        Assert.assertTrue("One centroid must land near the first group mean", nearA);
        Assert.assertTrue("One centroid must land near the second group mean", nearB);
    }

    @Test
    public void testLeafLevelEmittedWithFullClusterCount() throws Exception {
        // Regression for the leaf-level drop (ASTERIXDB-3760): for K >= 4 the hierarchy loop must keep the
        // K trained leaf centroids in the emitted structure. Before the fix they were re-keyed to map key
        // -1 and never emitted, collapsing the leaf level to ~sqrt(K) clusters.
        int k = 9;
        // Nine widely separated groups so k-means recovers exactly K non-empty leaf clusters.
        List<double[]> vectors = separatedGroups(k, 6);
        // A small frame forces at least one parent level, so the leaf level is a distinct deepest level.
        List<CentroidTuple> tuples = parseAll(runOperator(11L, k, 512, vectors));
        Assert.assertFalse("Operator must emit centroids", tuples.isEmpty());

        int maxTreeLevel = 0;
        for (CentroidTuple t : tuples) {
            maxTreeLevel = Math.max(maxTreeLevel, t.treeLevel);
        }
        Assert.assertTrue("Expected a multi-level hierarchy (leaves plus at least one parent level)",
                maxTreeLevel >= 1);

        int leafCount = 0;
        for (CentroidTuple t : tuples) {
            if (t.treeLevel == maxTreeLevel) {
                leafCount++;
            }
        }
        Assert.assertEquals("Deepest (leaf) level must carry all K trained centroids", k, leafCount);
    }

    // ------------------------------------------------------------------ harness

    /**
     * Runs both activities of the operator over the given vectors on a single partition and returns
     * copies of the emitted tuples.
     */
    private List<ITupleReference> runOperator(long seed, int k, int frameSize, List<double[]> vectors)
            throws Exception {
        return runOperator(seed, k, frameSize, vectors, DIM);
    }

    private List<ITupleReference> runOperator(long seed, int k, int frameSize, List<double[]> vectors,
            int declaredDimension) throws Exception {
        IOManager ioManager = createIoManager();
        try {
            IHyracksTaskContext ctx = mockTaskContext(frameSize, ioManager);

            RecordDescriptor inRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                    new AOrderedListSerializerDeserializer(DOUBLE_LIST_TYPE), IntegerSerializerDeserializer.INSTANCE });
            RecordDescriptor outRecDesc = createHierarchicalOutputRecordDescriptor();

            JobSpecification spec = new JobSpecification();
            HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor desc =
                    new HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(spec, outRecDesc, inRecDesc,
                            UUID.randomUUID(), UUID.randomUUID(), new ColumnAccessEvalFactory(0), k, 5,
                            VectorSimilarityMetric.EUCLIDEAN, declaredDimension, seed);

            List<IActivity> activities = new ArrayList<>();
            IActivityGraphBuilder graphBuilder = Mockito.mock(IActivityGraphBuilder.class);
            Mockito.doAnswer(invocation -> {
                activities.add((IActivity) invocation.getArguments()[1]);
                return null;
            }).when(graphBuilder).addActivity(Mockito.any(), Mockito.any());
            desc.contributeActivities(graphBuilder);
            Assert.assertEquals("Operator must contribute two activities", 2, activities.size());

            IRecordDescriptorProvider rdp = Mockito.mock(IRecordDescriptorProvider.class);
            Mockito.when(rdp.getInputRecordDescriptor(Mockito.any(), Mockito.anyInt())).thenReturn(inRecDesc);

            // Activity 1: materialize the sample and count tuples.
            IOperatorNodePushable materializer = activities.get(0).createPushRuntime(ctx, rdp, 0, 1);
            IFrameWriter input = materializer.getInputFrameWriter(0);
            input.open();
            feedVectors(ctx, input, vectors);
            input.close();

            // Activity 2: hierarchical clustering, emitting (treeLevel, centroidId, parent, embedding).
            List<ITupleReference> resultTuples = new ArrayList<>();
            IOperatorNodePushable clusterer = activities.get(1).createPushRuntime(ctx, rdp, 0, 1);
            clusterer.setOutputFrameWriter(0, new ResultFrameWriter(outRecDesc, resultTuples), outRecDesc);
            clusterer.initialize();
            return resultTuples;
        } finally {
            ioManager.close();
        }
    }

    private void feedVectors(IHyracksTaskContext ctx, IFrameWriter input, List<double[]> vectors) throws Exception {
        IFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender(frame, true);
        int pk = 0;
        for (double[] vector : vectors) {
            ArrayTupleBuilder builder = new ArrayTupleBuilder(2);
            byte[] listBytes = buildDoubleList(vector);
            builder.addField(listBytes, 0, listBytes.length);
            builder.addField(IntegerSerializerDeserializer.INSTANCE, pk++);
            if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                appender.write(input, true);
                if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                    throw new IllegalStateException("Input tuple too large for test frame");
                }
            }
        }
        appender.write(input, true);
    }

    private IOManager createIoManager() throws Exception {
        File workspace = tempFolder.newFolder();
        List<IODeviceHandle> devices = new ArrayList<>();
        devices.add(new IODeviceHandle(workspace, "."));
        return new IOManager(devices, new DefaultDeviceResolver(), 2, 10);
    }

    private IHyracksTaskContext mockTaskContext(int frameSize, IOManager ioManager) throws HyracksDataException {
        Map<Object, IStateObject> stateObjects = new HashMap<>();

        IHyracksJobletContext jobletContext = Mockito.mock(IHyracksJobletContext.class);
        Mockito.when(jobletContext.getJobId()).thenReturn(new JobId(0));
        Mockito.when(jobletContext.getServiceContext()).thenReturn(Mockito.mock(INCServiceContext.class));
        Mockito.when(jobletContext.createManagedWorkspaceFile(Mockito.anyString()))
                .thenAnswer(invocation -> ioManager.createWorkspaceFile((String) invocation.getArguments()[0]));

        IHyracksTaskContext ctx = Mockito.mock(IHyracksTaskContext.class);
        Mockito.when(ctx.getJobletContext()).thenReturn(jobletContext);
        Mockito.when(ctx.getInitialFrameSize()).thenReturn(frameSize);
        Mockito.when(ctx.getIoManager()).thenReturn(ioManager);
        Mockito.when(ctx.getWarningCollector()).thenReturn(NoOpWarningCollector.INSTANCE);
        Mockito.when(ctx.allocateFrame(Mockito.anyInt()))
                .thenAnswer(invocation -> ByteBuffer.allocate((int) invocation.getArguments()[0]));
        Mockito.when(ctx.reallocateFrame(Mockito.any(), Mockito.anyInt(), Mockito.anyBoolean()))
                .thenAnswer(invocation -> {
                    ByteBuffer oldBuffer = (ByteBuffer) invocation.getArguments()[0];
                    int newSize = (int) invocation.getArguments()[1];
                    boolean copy = (boolean) invocation.getArguments()[2];
                    ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
                    if (copy && oldBuffer != null) {
                        int oldPosition = oldBuffer.position();
                        oldBuffer.position(0);
                        newBuffer.put(oldBuffer);
                        newBuffer.position(oldPosition);
                    }
                    return newBuffer;
                });
        Mockito.doAnswer(invocation -> {
            IStateObject state = (IStateObject) invocation.getArguments()[0];
            stateObjects.put(state.getId(), state);
            return null;
        }).when(ctx).setStateObject(Mockito.any());
        Mockito.when(ctx.getStateObject(Mockito.any()))
                .thenAnswer(invocation -> stateObjects.get(invocation.getArguments()[0]));
        return ctx;
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Two well-separated groups of {@code perGroup} vectors each: one scattered around the origin
     * and one around (10, 10, 10, 10). Generation is fully deterministic.
     */
    private static List<double[]> twoClusterVectors(int perGroup) {
        List<double[]> vectors = new ArrayList<>();
        for (int i = 0; i < perGroup; i++) {
            double o = (i % 5) * 0.1;
            double p = (i % 7) * 0.05;
            vectors.add(new double[] { o, p, -o, p });
            vectors.add(new double[] { 10.0 + o, 10.0 + p, 10.0 - o, 10.0 + p });
        }
        return vectors;
    }

    /** {@code numGroups} widely separated clusters (100 apart) so k-means recovers exactly that many. */
    private static List<double[]> separatedGroups(int numGroups, int perGroup) {
        List<double[]> vectors = new ArrayList<>();
        for (int g = 0; g < numGroups; g++) {
            double base = g * 100.0;
            for (int i = 0; i < perGroup; i++) {
                double j = (i % 5) * 0.01; // tiny in-group jitter, far below the inter-group gap
                vectors.add(new double[] { base + j, base - j, base + 2 * j, base - 2 * j });
            }
        }
        return vectors;
    }

    private static double[] groupMean(List<double[]> vectors, double base) {
        double[] sum = new double[DIM];
        int count = 0;
        for (double[] v : vectors) {
            if (Math.abs(v[0] - base) < 5.0) {
                for (int d = 0; d < DIM; d++) {
                    sum[d] += v[d];
                }
                count++;
            }
        }
        for (int d = 0; d < DIM; d++) {
            sum[d] /= count;
        }
        return sum;
    }

    private static double euclidean(double[] a, double[] b) {
        double sum = 0.0;
        for (int d = 0; d < a.length; d++) {
            double diff = a[d] - b[d];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    private static byte[] buildDoubleList(double[] values) throws HyracksDataException {
        try {
            OrderedListBuilder listBuilder = new OrderedListBuilder();
            listBuilder.reset(DOUBLE_LIST_TYPE);
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

    private static List<CentroidTuple> parseAll(List<ITupleReference> tuples) throws Exception {
        List<CentroidTuple> parsed = new ArrayList<>();
        for (ITupleReference t : tuples) {
            parsed.add(parseTuple(t));
        }
        return parsed;
    }

    private static CentroidTuple parseTuple(ITupleReference t) throws HyracksDataException {
        Assert.assertEquals("Output tuples must have 4 fields", 4, t.getFieldCount());
        int treeLevel = IntegerPointable.getInteger(t.getFieldData(0), t.getFieldStart(0));
        int centroidId = IntegerPointable.getInteger(t.getFieldData(1), t.getFieldStart(1));
        int parentClusterId = IntegerPointable.getInteger(t.getFieldData(2), t.getFieldStart(2));

        ListAccessor listAccessor = new ListAccessor();
        listAccessor.reset(t.getFieldData(3), t.getFieldStart(3));
        double[] embedding = new double[listAccessor.size()];
        IPointable item = new VoidPointable();
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        try {
            for (int i = 0; i < embedding.length; i++) {
                listAccessor.getOrWriteItem(i, item, storage);
                embedding[i] = ADoubleSerializerDeserializer.getDouble(item.getByteArray(), item.getStartOffset() + 1);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        return new CentroidTuple(treeLevel, centroidId, parentClusterId, embedding);
    }

    private static void assertTuplesByteIdentical(String message, ITupleReference expected, ITupleReference actual) {
        Assert.assertEquals(message + " (field count)", expected.getFieldCount(), actual.getFieldCount());
        for (int f = 0; f < expected.getFieldCount(); f++) {
            byte[] e = fieldBytes(expected, f);
            byte[] a = fieldBytes(actual, f);
            Assert.assertArrayEquals(message + " (field " + f + ")", e, a);
        }
    }

    private static byte[] fieldBytes(ITupleReference t, int field) {
        byte[] copy = new byte[t.getFieldLength(field)];
        System.arraycopy(t.getFieldData(field), t.getFieldStart(field), copy, 0, copy.length);
        return copy;
    }

    /**
     * Creates a RecordDescriptor for the hierarchical clustering output format.
     * Format: <treeLevel, centroidId, parentClusterId, embedding>
     * @return RecordDescriptor with 4 fields: 3 integers + 1 AOrderedList of doubles
     */
    public static RecordDescriptor createHierarchicalOutputRecordDescriptor() {
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[4];

        // Field 0: Tree Level (int)
        fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE;

        // Field 1: Centroid ID (int)
        fieldSerdes[1] = IntegerSerializerDeserializer.INSTANCE;

        // Field 2: Parent Cluster ID (int)
        fieldSerdes[2] = IntegerSerializerDeserializer.INSTANCE;

        // Field 3: Embedding (AOrderedList of doubles)
        fieldSerdes[3] = new AOrderedListSerializerDeserializer(new AOrderedListType(ADOUBLE, "embedding"));

        return new RecordDescriptor(fieldSerdes);
    }
}
