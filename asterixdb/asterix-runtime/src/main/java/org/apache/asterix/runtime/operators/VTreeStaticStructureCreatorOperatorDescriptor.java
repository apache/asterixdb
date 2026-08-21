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

import static org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.vector.OptimizedScalarQuantizationCodec;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.LSMVTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Operator that creates VCTree static structure files using VCTreeStaticStructureBuilder.
 * Sink operator: consumes hierarchical K-means output and writes the static structure to the index.
 */
public class VTreeStaticStructureCreatorOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    /** Empty graph-neighbor list emitted on leaf tuples until upstream neighbor computation is wired in. */
    private static final byte[] EMPTY_NEIGHBOR_LIST = new byte[0];
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final int maxEntriesPerPage;
    private final float fillFactor;
    private final String distanceMetric;
    private final RecordDescriptor inputRecordDescriptor; // Store for CreateStructureActivity
    private final int[][] partitionsMap; // Maps task partition to storage partition(s)

    public VTreeStaticStructureCreatorOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, String distanceMetric, int[][] partitionsMap) {
        super(spec, 1, 0);
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.distanceMetric = distanceMetric;
        this.inputRecordDescriptor = inputRecordDescriptor;
        this.partitionsMap = partitionsMap;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        CreateStructureActivity sa = new CreateStructureActivity(new ActivityId(odId, 0));
        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);
    }

    protected class CreateStructureActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private static final Logger LOGGER = LogManager.getLogger();

        protected CreateStructureActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private final List<ByteBuffer> frameAccumulator = new ArrayList<>();
                private int tupleCount = 0;
                private FrameTupleReference tuple = new FrameTupleReference();
                private FrameTupleAccessor fta;
                private IScalarEvaluator levelEval;
                private IScalarEvaluator clusterIdEval;
                private IScalarEvaluator centroidIdEval;
                private IPointable levelVal;
                private IPointable clusterIdVal;
                private IPointable centroidIdVal;
                private Map<Integer, Integer> levelDistribution = null;
                private Map<String, Map<Integer, Integer>> clusterDistribution = null;

                // Quantization-related instance variables
                private int maxLevel = -1; // Will be set after buildStructureInfo()
                private int quantizationBits = 8; // Fallback until metadata load; overridden by SQ4/SQ8 bits from LSMVTreeLocalResource
                private float minQuantile = 0.0f;
                private float maxQuantile = 1.0f;
                private float alpha = 0.9f;
                private float confidenceInterval = 0.999f;
                private int sampleCount = 0;
                private boolean quantizationParamsLoaded = false;

                /** Set on producer failure, so {@code close()} does not build over a truncated stream. */
                @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
                private boolean upstreamFailed = false;

                // Get all storage partitions for this compute partition
                private final int[] storagePartitions = partitionsMap[partition];

                @Override
                public void open() throws HyracksDataException {
                    try {
                        EvaluatorContext evalCtx = new EvaluatorContext(ctx);
                        levelEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(evalCtx); // treeLevel
                        centroidIdEval = new ColumnAccessEvalFactory(1).createScalarEvaluator(evalCtx); // centroidId
                        clusterIdEval = new ColumnAccessEvalFactory(2).createScalarEvaluator(evalCtx); // parentClusterId
                        // Field 3 is embedding - handled separately in convertToVCTreeBuilderFormat

                        // Initialize pointables for evaluator results
                        levelVal = new VoidPointable();
                        clusterIdVal = new VoidPointable();
                        centroidIdVal = new VoidPointable();

                        // Use inputRecordDescriptor for reading input frames (hierarchical data)
                        fta = new FrameTupleAccessor(inputRecordDescriptor);

                        // Read quantization parameters from the LSMVTreeLocalResource
                        readQuantizationParamsFromMetadata(ctx);

                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }

                /**
                 * Reads quantization parameters from the LSMVTreeLocalResource metadata.
                 */
                private void readQuantizationParamsFromMetadata(IHyracksTaskContext taskCtx)
                        throws HyracksDataException {
                    try {
                        // Create index helper to access the resource (use first storage partition to read metadata)
                        IIndexDataflowHelper tempHelper = indexHelperFactory
                                .create(taskCtx.getJobletContext().getServiceContext(), storagePartitions[0]);

                        // Get the local resource from the helper
                        LocalResource localResource = tempHelper.getResource();
                        if (localResource != null) {
                            IResource resource = localResource.getResource();
                            if (resource instanceof DatasetLocalResource) {
                                DatasetLocalResource datasetResource = (DatasetLocalResource) resource;
                                IResource delegate = datasetResource.getResource();
                                if (delegate instanceof LSMVTreeLocalResource) {
                                    LSMVTreeLocalResource vcTreeResource = (LSMVTreeLocalResource) delegate;

                                    // Read quantization parameters with defaults
                                    Integer bits = vcTreeResource.getBits();
                                    if (bits != null) {
                                        quantizationBits = bits;
                                    }

                                    Float ci = vcTreeResource.getConfidenceInterval();
                                    if (ci != null) {
                                        confidenceInterval = ci;
                                    }

                                    Float minQ = vcTreeResource.getMinQuantile();
                                    if (minQ != null) {
                                        minQuantile = minQ;
                                    }

                                    Float maxQ = vcTreeResource.getMaxQuantile();
                                    if (maxQ != null) {
                                        maxQuantile = maxQ;
                                    }

                                    Float a = vcTreeResource.getAlpha();
                                    if (a != null) {
                                        alpha = a;
                                    }

                                    Integer sc = vcTreeResource.getSampleCount();
                                    if (sc != null) {
                                        sampleCount = sc;
                                    }

                                    quantizationParamsLoaded = vcTreeResource.hasQuantizationParams();
                                }
                            }
                        }

                    } catch (Exception e) {
                        // Not fatal - will use default parameters
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    fta.reset(buffer);

                    // Accumulate frames for batch processing
                    frameAccumulator.add(buffer.duplicate());

                    // Process tuples in this frame
                    for (int i = 0; i < fta.getTupleCount(); i++) {
                        tuple.reset(fta, i);
                        processTuple(tuple);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    upstreamFailed = true;
                }

                private void processTuple(ITupleReference tuple) throws HyracksDataException {
                    try {
                        // Extract tuple data for structure analysis from 4-field hierarchical format
                        // Format: [treeLevel, centroidId, parentClusterId, embedding]
                        FrameTupleReference frameTuple = (FrameTupleReference) tuple;
                        levelEval.evaluate(frameTuple, levelVal); // treeLevel
                        centroidIdEval.evaluate(frameTuple, centroidIdVal); // centroidId
                        clusterIdEval.evaluate(frameTuple, clusterIdVal); // parentClusterId

                        int level = IntegerPointable.getInteger(levelVal.getByteArray(), levelVal.getStartOffset());
                        int centroidId = IntegerPointable.getInteger(centroidIdVal.getByteArray(),
                                centroidIdVal.getStartOffset());
                        int clusterId =
                                IntegerPointable.getInteger(clusterIdVal.getByteArray(), clusterIdVal.getStartOffset());

                        // Track structure for analysis
                        if (levelDistribution == null) {
                            levelDistribution = new HashMap<>();
                            clusterDistribution = new HashMap<>();
                        }

                        levelDistribution.put(level, levelDistribution.getOrDefault(level, 0) + 1);

                        String levelKey = "Level_" + level;
                        Map<Integer, Integer> levelClusters =
                                clusterDistribution.computeIfAbsent(levelKey, k -> new HashMap<>());
                        levelClusters.put(clusterId, levelClusters.getOrDefault(clusterId, 0) + 1);

                        tupleCount++;

                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }

                /**
                 * Convert 4-field tuple [treeLevel, centroidId, parentClusterId, embedding] to:
                 * - For leaf level (level == maxLevel): 4-field tuple
                 *   [centroidId, embedding, quantizedBytes, neighborList] (neighborList emitted empty for now)
                 * - For interior levels: 2-field tuple [centroidId, embedding]
                 * Parsing embeddings using the same logic as HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.
                 */
                private ITupleReference convertToVCTreeBuilderFormat(ITupleReference inputTuple)
                        throws HyracksDataException {
                    try {
                        int level =
                                IntegerPointable.getInteger(inputTuple.getFieldData(0), inputTuple.getFieldStart(0));
                        // Extract centroidId from field 1 (second field)
                        int centroidId =
                                IntegerPointable.getInteger(inputTuple.getFieldData(1), inputTuple.getFieldStart(1));

                        // Extract embedding from field 3 (fourth field) using same logic as HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor
                        byte[] embeddingData = inputTuple.getFieldData(3);
                        int embeddingStart = inputTuple.getFieldStart(3);

                        // Parse the embedding using ListAccessor (same as in HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor)
                        ListAccessor listAccessor = new ListAccessor();
                        listAccessor.reset(embeddingData, embeddingStart);

                        // Extract double values from the AOrderedList
                        double[] embedding = new double[listAccessor.size()];
                        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                        VoidPointable tempVal = new VoidPointable();

                        for (int i = 0; i < listAccessor.size(); i++) {
                            listAccessor.getOrWriteItem(i, tempVal, storage);
                            embedding[i] = extractNumericValue(tempVal);
                        }

                        if (embedding == null || embedding.length == 0) {
                            throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                                    "Failed to extract an embedding from a centroid tuple");
                        }

                        // Check if this is a leaf level (maxLevel) - apply quantization only to leaf nodes
                        boolean isLeafLevel = (maxLevel >= 0 && level == maxLevel);

                        if (isLeafLevel && quantizationParamsLoaded) {
                            // Leaf level: quantize and create 3-field tuple [centroidId, embedding, quantizedBytes]
                            return createLeafTupleWithQuantization(centroidId, embedding);
                        } else {
                            // Interior level: create 2-field tuple [centroidId, embedding]
                            return createInteriorTuple(centroidId, embedding);
                        }

                    } catch (Exception e) {
                        throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, e,
                                "Failed to convert a centroid tuple to the static-structure builder format");
                    }
                }

                /**
                 * Creates a 2-field tuple for interior nodes: [centroidId, embedding]
                 */
                private ITupleReference createInteriorTuple(int centroidId, double[] embedding)
                        throws HyracksDataException {
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
                    ArrayTupleReference tupleRef = new ArrayTupleReference();

                    @SuppressWarnings("rawtypes")
                    ISerializerDeserializer[] fieldSerdes =
                            new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, // centroid ID
                                    DoubleArraySerializerDeserializer.INSTANCE // embedding as double array
                    };
                    Object[] fieldValues = new Object[] { centroidId, embedding };

                    TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);
                    return tupleRef;
                }

                /**
                 * Creates a 4-field tuple for leaf nodes with quantization:
                 * [centroidId, embedding, quantizedBytes, neighborList].
                 * The quantizedBytes field holds per-dimension scalar codes only (not corrective multiplier).
                 * The neighborList field is the graph-style leaf-neighbor list; it is emitted empty for now
                 * (upstream neighbor computation is not yet wired in) and is later filled/resolved by the
                 * static-structure builder's two-pass design.
                 */
                private ITupleReference createLeafTupleWithQuantization(int centroidId, double[] embedding)
                        throws HyracksDataException {
                    try {
                        // Create quantization params
                        OptimizedScalarQuantizationCodec.Params quantParams =
                                new OptimizedScalarQuantizationCodec.Params(quantizationBits, embedding.length,
                                        sampleCount, confidenceInterval, minQuantile, maxQuantile, alpha);

                        OptimizedScalarQuantizationCodec.SimilarityFunction simFunc =
                                OptimizedScalarQuantizationCodec.fromDistanceMetric(distanceMetric);

                        // Quantize the embedding
                        OptimizedScalarQuantizationCodec.QuantizedVector quantizedResult =
                                OptimizedScalarQuantizationCodec.quantizeVector(embedding, quantParams, simFunc);

                        // Extract quantized bytes as byte[] (assuming bits <= 8)
                        byte[] quantizedBytes;
                        if (quantizedResult.quantizedBytes instanceof byte[]) {
                            quantizedBytes = (byte[]) quantizedResult.quantizedBytes;
                        } else if (quantizedResult.quantizedBytes instanceof short[]) {
                            // Convert short[] to byte[] for storage (2 bytes per short)
                            short[] shorts = (short[]) quantizedResult.quantizedBytes;
                            quantizedBytes = new byte[shorts.length * 2];
                            for (int i = 0; i < shorts.length; i++) {
                                quantizedBytes[i * 2] = (byte) (shorts[i] >> 8);
                                quantizedBytes[i * 2 + 1] = (byte) shorts[i];
                            }
                        } else if (quantizedResult.quantizedBytes instanceof int[]) {
                            // Convert int[] to byte[] for storage (4 bytes per int)
                            int[] ints = (int[]) quantizedResult.quantizedBytes;
                            quantizedBytes = new byte[ints.length * 4];
                            for (int i = 0; i < ints.length; i++) {
                                quantizedBytes[i * 4] = (byte) (ints[i] >> 24);
                                quantizedBytes[i * 4 + 1] = (byte) (ints[i] >> 16);
                                quantizedBytes[i * 4 + 2] = (byte) (ints[i] >> 8);
                                quantizedBytes[i * 4 + 3] = (byte) ints[i];
                            }
                        } else {
                            throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                                    "Unexpected quantized-bytes type "
                                            + quantizedResult.quantizedBytes.getClass().getName());
                        }

                        // Create 4-field tuple: [centroidId, embedding, quantizedBytes, neighborList].
                        // neighborList is emitted empty for now; the byte layout is in place so the
                        // two-pass static-structure builder can later fill and resolve it.
                        byte[] neighborList = EMPTY_NEIGHBOR_LIST;

                        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
                        ArrayTupleReference tupleRef = new ArrayTupleReference();

                        @SuppressWarnings("rawtypes")
                        ISerializerDeserializer[] fieldSerdes =
                                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, // centroid ID
                                        DoubleArraySerializerDeserializer.INSTANCE, // embedding as double array
                                        ByteArraySerializerDeserializer.INSTANCE, // quantized embedding bytes
                                        ByteArraySerializerDeserializer.INSTANCE // graph neighbor list (empty for now)
                        };
                        Object[] fieldValues = new Object[] { centroidId, embedding, quantizedBytes, neighborList };

                        TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);

                        return tupleRef;

                    } catch (Exception e) {
                        // Fall back to interior tuple format if quantization fails
                        return createInteriorTuple(centroidId, embedding);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    if (upstreamFailed) {
                        // Only a prefix of the centroids arrived, if any. Building on that would fail here
                        // and replace the producer's real error.
                        return;
                    }
                    try {
                        createStaticStructure();
                    } finally {
                        // indexHelper cleanup is now handled inside createStaticStructure per storage partition
                    }

                }

                /**
                 * Build structure information from collected hierarchical data.
                 * Creates the arrays needed by VCTreeStaticStructureBuilder.
                 */
                private StructureInfo buildStructureInfo() throws HyracksDataException {

                    List<Integer> clustersPerLevel = new ArrayList<>();
                    List<List<Integer>> centroidsPerCluster = new ArrayList<>();

                    if (levelDistribution == null || levelDistribution.isEmpty()) {
                        throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                                "No training vectors were found. Verify that the indexed field exists in the "
                                        + "collection and contains vector data, and that sampling produced enough "
                                        + "vectors for train_list_fraction");
                    }

                    // Find max level
                    int maxLevel = levelDistribution.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);

                    // Process each level
                    for (int level = 0; level <= maxLevel; level++) {
                        String levelKey = "Level_" + level;
                        Map<Integer, Integer> levelClusters = clusterDistribution.get(levelKey);

                        if (levelClusters != null && !levelClusters.isEmpty()) {
                            // For hierarchical structure, we need to determine the actual clustering structure
                            // Level 0: All centroids form 1 cluster (root level)
                            // Level 1+: Centroids are grouped by their parent cluster IDs

                            int clusterCount;
                            List<Integer> centroidsInClusters = new ArrayList<>();

                            if (level == 0) {
                                // Root level: all centroids form 1 cluster
                                clusterCount = 1;
                                int totalCentroids = levelClusters.values().stream().mapToInt(Integer::intValue).sum();
                                centroidsInClusters.add(totalCentroids);
                            } else {
                                // Interior levels: group by parent cluster ID
                                clusterCount = levelClusters.size();

                                // Sort cluster IDs to ensure consistent ordering
                                List<Integer> sortedClusterIds = new ArrayList<>(levelClusters.keySet());
                                sortedClusterIds.sort(Integer::compareTo);

                                for (int clusterId : sortedClusterIds) {
                                    int centroidCount = levelClusters.get(clusterId);
                                    centroidsInClusters.add(centroidCount);
                                }
                            }

                            clustersPerLevel.add(clusterCount);
                            centroidsPerCluster.add(centroidsInClusters);
                        } else {
                            // Empty level
                            clustersPerLevel.add(0);
                            centroidsPerCluster.add(new ArrayList<>());
                        }
                    }

                    return new StructureInfo(clustersPerLevel, centroidsPerCluster);
                }

                private void createStaticStructure() throws HyracksDataException {
                    try {
                        // Analyze collected data to determine structure (once for all partitions)
                        StructureInfo structureInfo = buildStructureInfo();
                        List<Integer> clustersPerLevel = structureInfo.clustersPerLevel;
                        List<List<Integer>> centroidsPerCluster = structureInfo.centroidsPerCluster;

                        // Store maxLevel for use in convertToVCTreeBuilderFormat (leaf-only quantization)
                        maxLevel = levelDistribution.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);

                        // Pre-convert all tuples once (identical for all storage partitions)
                        List<ITupleReference> convertedTuples = new ArrayList<>();
                        for (ByteBuffer frameBuffer : frameAccumulator) {
                            FrameTupleAccessor frameFta = new FrameTupleAccessor(inputRecordDescriptor);
                            frameFta.reset(frameBuffer);
                            for (int i = 0; i < frameFta.getTupleCount(); i++) {
                                tuple.reset(frameFta, i);
                                convertedTuples.add(convertToVCTreeBuilderFormat(tuple));
                            }
                        }

                        // Build the static structure on EACH storage partition
                        for (int sp = 0; sp < storagePartitions.length; sp++) {
                            int storagePartition = storagePartitions[sp];
                            buildStaticStructureOnPartition(storagePartition, clustersPerLevel, centroidsPerCluster,
                                    convertedTuples);
                        }

                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }

                /**
                 * Build the static structure on a single storage partition.
                 */
                @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Release the LOAD I/O declaration on the failure path")
                private void buildStaticStructureOnPartition(int storagePartition, List<Integer> clustersPerLevel,
                        List<List<Integer>> centroidsPerCluster, List<ITupleReference> convertedTuples)
                        throws HyracksDataException {
                    IIndexDataflowHelper partitionHelper = null;
                    try {
                        partitionHelper =
                                indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), storagePartition);

                        LocalResource resource = partitionHelper.getResource();

                        partitionHelper.open();

                        IIndex indexInstance = partitionHelper.getIndexInstance();
                        if (!(indexInstance instanceof ILSMIndex)) {
                            throw HyracksDataException.create(ILLEGAL_STATE,
                                    "The vector index resource is not an ILSMIndex, but a "
                                            + (indexInstance != null ? indexInstance.getClass().getName() : "null"));
                        }
                        ILSMIndex partitionLsmIndex = (ILSMIndex) indexInstance;

                        if (!(partitionLsmIndex instanceof LSMVTree)) {
                            throw HyracksDataException.create(ILLEGAL_STATE,
                                    "The vector index resource is not an LSMVTree, but a "
                                            + partitionLsmIndex.getClass().getName());
                        }

                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID,
                                LSMComponentId.DEFAULT_COMPONENT_ID);
                        parameters.put("numLevels", clustersPerLevel.size());
                        parameters.put("clustersPerLevel", clustersPerLevel);
                        parameters.put("centroidsPerCluster", centroidsPerCluster);
                        parameters.put("maxEntriesPerPage", maxEntriesPerPage);

                        IIndexBulkLoader partitionBulkLoader =
                                partitionLsmIndex.createBulkLoader(fillFactor, false, 0L, false, parameters);

                        // createBulkLoader() has already declared an active LOAD I/O operation on the
                        // dataset (AbstractLSMIndex.createBulkLoader -> ioOpCallback.scheduled). That
                        // declaration is released ONLY by end() or abort(). Leaving this scope by any other
                        // route leaks it permanently, and a leaked I/O op is not a slow leak: the next
                        // DatasetLifecycleManager.unregister() -- for instance the drop that rolls this
                        // CREATE INDEX back -- blocks forever in DatasetInfo.waitForIO() while holding the
                        // DatasetLifecycleManager monitor, which wedges every other index operation on the
                        // node with no timeout. Mirror IndexBulkLoadOperatorNodePushable.closeBulkLoaders().
                        //
                        // endInvoked is set BEFORE end() on purpose: end() releases the declaration in its
                        // own finally even when it throws, so aborting a failed end() would release it a
                        // second time and drive numActiveIOOps negative.
                        boolean endInvoked = false;
                        try {
                            int totalTuplesProcessed = 0;
                            for (ITupleReference convertedTuple : convertedTuples) {
                                partitionBulkLoader.add(convertedTuple);
                                totalTuplesProcessed++;
                            }

                            endInvoked = true;
                            partitionBulkLoader.end();
                            LOGGER.info("Static structure finalized on storage partition {} ({} tuples)",
                                    storagePartition, totalTuplesProcessed);
                        } finally {
                            if (!endInvoked) {
                                try {
                                    partitionBulkLoader.abort();
                                } catch (Exception abortFailure) {
                                    // Never mask the failure that brought us here; the abort is best effort.
                                    LOGGER.warn("Failed to abort the static-structure bulk load on storage "
                                            + "partition {}", storagePartition, abortFailure);
                                }
                            }
                        }

                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    } finally {
                        if (partitionHelper != null) {
                            try {
                                partitionHelper.close();
                            } catch (Exception e) {
                                // Don't throw - cleanup failures shouldn't mask original exceptions
                            }
                        }
                    }
                }

            };
        }
    }

    /**
     * Helper class to hold structure information for VCTreeStaticStructureBuilder.
     */
    private static class StructureInfo {
        public final List<Integer> clustersPerLevel;
        public final List<List<Integer>> centroidsPerCluster;

        public StructureInfo(List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster) {
            this.clustersPerLevel = clustersPerLevel;
            this.centroidsPerCluster = centroidsPerCluster;
        }
    }

    /**
     * Extracts numeric value from a pointable (helper method for parsing embeddings).
     * Same logic as in HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.
     */
    private static double extractNumericValue(IPointable pointable) throws HyracksDataException {
        byte[] data = pointable.getByteArray();
        int start = pointable.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[start]);

        switch (typeTag) {
            case DOUBLE:
                return DoublePointable.getDouble(data, start + 1);
            case FLOAT:
                return FloatPointable.getFloat(data, start + 1);
            case INTEGER:
                return IntegerPointable.getInteger(data, start + 1);
            case BIGINT:
                return LongPointable.getLong(data, start + 1);
            default:
                throw new RuntimeDataException(ErrorCode.UNSUPPORTED_VECTOR_ELEMENT_TYPE, typeTag);
        }
    }

}
