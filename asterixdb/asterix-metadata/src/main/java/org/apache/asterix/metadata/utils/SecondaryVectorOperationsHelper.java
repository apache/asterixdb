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
package org.apache.asterix.metadata.utils;

import static org.apache.asterix.om.types.ATypeTag.ARRAY;
import static org.apache.asterix.om.types.BuiltinType.*;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.dataflow.data.common.AOrderedListVectorBinaryAccessorFactory;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.std.QuantizationConstantsAggregateDescriptor;
import org.apache.asterix.runtime.operators.HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.asterix.runtime.operators.VTreeBulkLoaderAndGroupingOperatorDescriptor;
import org.apache.asterix.runtime.operators.VTreeStaticStructureCreatorOperatorDescriptor;
import org.apache.asterix.runtime.operators.VectorComponentExtractorOperatorDescriptor;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.FixedLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionerFactory;
import org.apache.hyracks.dataflow.common.data.partition.OnePartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNBroadcastConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.QuantizedIndexBuilderFactory;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.QuantizedIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SecondaryVectorOperationsHelper extends SecondaryTreeIndexOperationsHelper {

    private static final Logger LOGGER = LogManager.getLogger();
    private RecordDescriptor recordDesc;
    private static final float DEFAULT_CONFIDENCE_INTERVAL = 0.99f;
    // Must match VectorIndexDeclUtil.VECTOR_INDEX_DEFAULT_EPSILON (injected into the WITH node at DDL validation).
    private static final double DEFAULT_EPSILON = 0.25;
    /**
     * Default {@code quantization} label when omitted in the vector index WITH clause; must match
     * {@code VectorIndexDeclUtil.VECTOR_INDEX_DEFAULT_QUANTIZATION} in {@code asterix-lang-common}.
     */
    private static final String DEFAULT_VECTOR_INDEX_QUANTIZATION = "SQ8";
    /** Bit width for {@link #DEFAULT_VECTOR_INDEX_QUANTIZATION} (SQ8). */
    private static final int DEFAULT_QUANTIZATION_BITS_SQ8 = 8;
    private static final Set<String> ALLOWED_VECTOR_INDEX_QUANTIZATION = Set.of("SQ4", "SQ8");
    /** Minimum train-list sample size for static-structure build; below this after clamp → full scan. */
    private static final int TRAIN_LIST_MIN_SAMPLE_SIZE = 10000;
    /** Maximum train-list sample size cap for static-structure build. */
    private static final int TRAIN_LIST_MAX_SAMPLE_SIZE = 1000000;
    private IVTreeBinaryAccessorFactory vectorAccessorFactory;

    protected SecondaryVectorOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    /**
     * Reads the mandatory {@code dimension} WITH parameter. IndexTupleTranslator enforces it at persist
     * time, so a missing/non-positive value indicates a corrupt index rather than a defaultable case.
     */
    private static int requireDimension(AdmObjectNode withObjectNode) {
        int dim = (withObjectNode != null) ? withObjectNode.getOptionalInt("dimension", -1) : -1;
        if (dim <= 0) {
            throw new IllegalStateException("Vector index is missing the required positive 'dimension' parameter");
        }
        return dim;
    }

    @Override
    public void init() throws AlgebricksException {
        super.init();
        recordDesc = dataset.getPrimaryRecordDescriptor(metadataProvider);

        // Initialize vector accessor factory for extracting vectors from ADM ordered lists
        vectorAccessorFactory = new AOrderedListVectorBinaryAccessorFactory();
    }

    /**
     * Get the vector accessor factory for extracting vectors from tuples.
     * This factory is passed to the Hyracks layer to handle ADM-specific vector deserialization.
     */
    public IVTreeBinaryAccessorFactory getVectorAccessorFactory() {
        return vectorAccessorFactory;
    }

    /**
     * Resolves {@code quantization} from metadata: missing or blank defaults to SQ8 (same as DDL);
     * non-empty values must be SQ4 or SQ8 (case-insensitive), returned uppercase.
     */
    private String resolveEffectiveQuantizationLabel(String raw) throws CompilationException {
        if (raw == null) {
            return DEFAULT_VECTOR_INDEX_QUANTIZATION;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return DEFAULT_VECTOR_INDEX_QUANTIZATION;
        }
        String qNorm = trimmed.toUpperCase(Locale.ROOT);
        if (!ALLOWED_VECTOR_INDEX_QUANTIZATION.contains(qNorm)) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, sourceLoc,
                    "Invalid `quantization` parameter value. Allowed values: SQ4, SQ8");
        }
        return qNorm;
    }

    private static int bitsForQuantizationLabel(String qNorm) {
        return "SQ4".equals(qNorm) ? 4 : DEFAULT_QUANTIZATION_BITS_SQ8;
    }

    private static int clampTrainListSampleSize(int sampleSize, long datasetCardinality) {
        if (datasetCardinality > 0) {
            sampleSize = (int) Math.min(sampleSize, datasetCardinality);
        }
        sampleSize = Math.max(sampleSize, TRAIN_LIST_MIN_SAMPLE_SIZE);
        sampleSize = Math.min(sampleSize, TRAIN_LIST_MAX_SAMPLE_SIZE);
        if (datasetCardinality > 0) {
            sampleSize = (int) Math.min(sampleSize, datasetCardinality);
        }
        return sampleSize;
    }

    private static boolean useFullScanForTrainList(int sampleSize) {
        return sampleSize < TRAIN_LIST_MIN_SAMPLE_SIZE;
    }

    @Override
    public JobSpecification buildStaticStructureJobSpec() throws AlgebricksException {
        IDataFormat format = metadataProvider.getDataFormat();
        int nFields = recordDesc.getFieldCount();
        int[] columns = new int[nFields];
        for (int i = 0; i < nFields; i++) {
            columns[i] = i;
        }
        ISerializerDeserializerProvider serdeProvider = format.getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = format.getTypeTraitProvider();

        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);
        // job spec: key provider -> primary idx scan -> cast assign -> k-means -> static structure creator
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // Primary-scan projector: ROW reads full records; COLUMN projects fields from indexExpectedType.
        ITupleProjectorFactory projectorFactory =
                IndexUtil.createPrimaryIndexScanTupleProjectorFactory(dataset.getDatasetFormatInfo(),
                        indexDetails.getIndexExpectedType(), itemType, metaType, numPrimaryKeys);

        // ============ SAMPLING OR FULL SCAN based on sampleSize threshold ============
        // Extract sampling parameters from WITH clause (train_list_fraction only; validated at compile time)
        AdmObjectNode withObjectNodeForSampling = indexDetails.getWithObjectNode();
        double trainListFraction = (withObjectNodeForSampling != null)
                ? withObjectNodeForSampling.getOptionalDouble("train_list_fraction", 0.1) : 0.1;
        // sample_seed is not a supported WITH field (not in VectorIndexDeclUtil.ALLOWED_VECTOR_INDEX_WITH_FIELDS),
        // so it was never actually settable; deterministic training is driven by compiler.vector.trainseed instead.
        long sampleSeed = System.currentTimeMillis();

        // Retrieve cardinality from sample index metadata (needed for fraction-based sample size)
        Index sampleIndex = metadataProvider.findSampleIndex(dataset.getDatabaseName(), dataset.getDataverseName(),
                dataset.getDatasetName());
        if (sampleIndex == null || !(sampleIndex.getIndexDetails() instanceof Index.SampleIndexDetails)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Vector Index requires ANALYZE statement prior to CREATE INDEX DDL.");
        }
        Index.SampleIndexDetails sampleDetails = (Index.SampleIndexDetails) sampleIndex.getIndexDetails();
        long datasetCardinality = sampleDetails.getSourceCardinality();

        int sampleSize = 0;
        if (datasetCardinality <= 0) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "train_list_fraction requires ANALYZE DATASET to be run first to obtain dataset cardinality.");
        }
        sampleSize = (int) Math.max(1, datasetCardinality * trainListFraction);

        // Clamp sample size before deciding between full scan vs sampling.
        // Minimum TRAIN_LIST_MIN_SAMPLE_SIZE; maximum TRAIN_LIST_MAX_SAMPLE_SIZE.
        // Full scan if dataset cardinality is below TRAIN_LIST_MIN_SAMPLE_SIZE after clamp.
        sampleSize = clampTrainListSampleSize(sampleSize, datasetCardinality);
        boolean useFullScan = useFullScanForTrainList(sampleSize);

        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        int numPartitions = partitioningProperties.getNumberOfPartitions();

        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp;
        if (useFullScan) {
            targetOp = DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        } else {
            int sampleCardinalityPerPartition = Math.max(1, sampleSize / numPartitions);
            targetOp = DatasetUtil.createSampleScanOp(spec, metadataProvider, dataset, sampleCardinalityPerPartition,
                    sampleSeed, projectorFactory);
        }
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        sourceOp = targetOp;
        // primary index -> cast assign op (produces the secondary index entry)
        targetOp = createAssignOp(spec, numSecondaryKeys, recordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        UUID sampleUUID = UUID.randomUUID();
        UUID tupleCountUUID = UUID.randomUUID();

        // Extract K value from WITH clause
        AdmObjectNode withObjectNode = indexDetails.getWithObjectNode();
        int K = (int) Math.sqrt((double) datasetCardinality / numPartitions); // default value
        if (withObjectNode != null) {
            K = withObjectNode.getOptionalInt("num_clusters",
                    (int) Math.sqrt((double) datasetCardinality / numPartitions));
        }

        // Distance metric from index DDL (WITH similarity "euclidean"|"cosine"|"cosine similarity"|etc.).
        // For cosine, embeddings must be L2-normalized to unit length before insert; the engine does not normalize.
        String distanceMetric = (withObjectNode != null) ? withObjectNode.getOptionalString("similarity", "") : "";

        int vectorDimension = requireDimension(withObjectNode);

        int maxScalableKmeansIter = 2;

        // Create record descriptor for hierarchical k-means output (level, clusterId, centroidId, embedding)
        ISerializerDeserializer[] hierarchicalSerde = new ISerializerDeserializer[4];
        ITypeTraits[] hierarchicalTraits = new ITypeTraits[4];

        // Level (int)
        hierarchicalSerde[0] = serdeProvider.getSerializerDeserializer(AINT32);
        hierarchicalTraits[0] = typeTraitProvider.getTypeTrait(AINT32);

        // ClusterId (int)
        hierarchicalSerde[1] = serdeProvider.getSerializerDeserializer(AINT32);
        hierarchicalTraits[1] = typeTraitProvider.getTypeTrait(AINT32);

        // CentroidId (int)
        hierarchicalSerde[2] = serdeProvider.getSerializerDeserializer(AINT32);
        hierarchicalTraits[2] = typeTraitProvider.getTypeTrait(AINT32);

        // Embedding (float array)
        hierarchicalSerde[3] = serdeProvider.getSerializerDeserializer(new AOrderedListType(ADOUBLE, "embedding"));
        hierarchicalTraits[3] = typeTraitProvider.getTypeTrait(new AOrderedListType(ADOUBLE, "embedding"));

        RecordDescriptor hierarchicalRecDesc = new RecordDescriptor(hierarchicalSerde, hierarchicalTraits);

        // ====== STATIC STRUCTURE JOB: K-MEANS → STATIC STRUCTURE CREATION ======

        // Training RNG seed: overridable per request (SET `compiler.vector.trainseed` "42") so CI /
        // regression tests get reproducible centroids; defaults to a fresh nanoTime seed otherwise.
        long trainSeed = System.nanoTime();
        Object trainSeedCfg = metadataProvider.getConfig().get(CompilerProperties.COMPILER_VECTOR_TRAINSEED_KEY);
        if (trainSeedCfg != null) {
            try {
                trainSeed = Long.parseLong(String.valueOf(trainSeedCfg).trim());
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid {} '{}', using a random seed", CompilerProperties.COMPILER_VECTOR_TRAINSEED_KEY,
                        trainSeedCfg);
            }
        }

        HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor candidates =
                new HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(spec, hierarchicalRecDesc, secondaryRecDesc,
                        sampleUUID, tupleCountUUID, new ColumnAccessEvalFactory(0), K, maxScalableKmeansIter,
                        distanceMetric, vectorDimension, trainSeed);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, candidates,
                primaryPartitionConstraint);
        targetOp = candidates;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        sourceOp = targetOp;

        VTreeStaticStructureCreatorOperatorDescriptor vcTreeCreator =
                new VTreeStaticStructureCreatorOperatorDescriptor(spec, dataflowHelperFactory, 100, 0.7f,
                        hierarchicalRecDesc, distanceMetric, partitioningProperties.getComputeStorageMap());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, vcTreeCreator,
                primaryPartitionConstraint);
        targetOp = vcTreeCreator;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        spec.addRoot(targetOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);

        // Job spec: key provider -> primary idx scan -> cast assign -> bulk loader and grouping -> sink
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // Primary-scan projector: ROW reads full records; COLUMN projects fields from indexExpectedType.
        ITupleProjectorFactory projectorFactory =
                IndexUtil.createPrimaryIndexScanTupleProjectorFactory(dataset.getDatasetFormatInfo(),
                        indexDetails.getIndexExpectedType(), itemType, metaType, numPrimaryKeys);

        // dummy key provider -> primary index scan
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp =
                DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        sourceOp = targetOp;
        // primary index -> cast assign op (produces the secondary index entry)
        targetOp = createAssignOp(spec, numSecondaryKeys, recordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        sourceOp = targetOp;

        // Extract WITH clause parameters early (needed for output record descriptor construction).
        // For cosine similarity, embeddings and query vectors must be L2-normalized to unit length before use.
        AdmObjectNode withObjectNode = indexDetails.getWithObjectNode();
        String distanceMetric = (withObjectNode != null) ? withObjectNode.getOptionalString("similarity", "") : "";
        int vectorDimension = requireDimension(withObjectNode);
        String quantization = (withObjectNode != null) ? withObjectNode.getOptionalString("quantization", null) : null;
        double levelwiseEpsilon = (withObjectNode != null)
                ? withObjectNode.getOptionalDouble("epsilon", DEFAULT_EPSILON) : DEFAULT_EPSILON;
        // Cross-pollination: at bulk-load, write each record into the M closest leaf centroids (M=1 = legacy).
        int crossPollinationM =
                (withObjectNode != null) ? Math.max(1, withObjectNode.getOptionalInt("cross_pollination_m", 1)) : 1;
        // RNG (relative neighborhood graph) factor for SPTAG-style diversity among replicas. 1.0 =
        // canonical SPTAG. Validated to be positive finite at DDL time (VectorIndexDeclUtil).
        double rngFactor = (withObjectNode != null) ? withObjectNode.getOptionalDouble("rng_factor", 1.0) : 1.0;
        final boolean isQuantized;
        if (withObjectNode != null) {
            resolveEffectiveQuantizationLabel(quantization);
            isQuantized = true;
        } else {
            isQuantized = false;
        }

        // Create output record descriptor for VTreeBulkLoaderAndGroupingOperatorDescriptor
        // secondaryRecDesc format: [embedding, include_fields..., pk...]
        // numSecondaryKeys = 1 (embedding) + numIncludeFields
        // So in secondaryRecDesc:
        //   - Field 0: embedding (skipped in output)
        //   - Fields 1 to numSecondaryKeys-1: include fields
        //   - Fields numSecondaryKeys to numSecondaryKeys+numPrimaryKeys-1: primary keys
        int numIncludeFieldsForOutput = numSecondaryKeys - 1; // exclude embedding

        // Number of secondary fields depends on quantization:
        // Non-quantized: [distance, centroidId, pk..., includes...] → 2 secondary fields
        // Quantized: [distance, centroidId, quantized_distance, quantized_embedding, pk..., includes...] → 4
        // IMPORTANT: centroidId MUST be field 1 in both cases so that sortFields={1,0}
        // and extractCentroidId(field[1]) work unchanged for quantized and non-quantized.
        int numOutputSecondaryFields = isQuantized ? 4 : 2;

        ISerializerDeserializer[] outputRecFields =
                new ISerializerDeserializer[numOutputSecondaryFields + numPrimaryKeys + numIncludeFieldsForOutput];
        ITypeTraits[] outputTypeTraits =
                new ITypeTraits[numOutputSecondaryFields + numPrimaryKeys + numIncludeFieldsForOutput];

        // Field 0: Distance (raw double - 8 bytes, no type tag)
        outputRecFields[0] = DoubleSerializerDeserializer.INSTANCE;
        outputTypeTraits[0] = new FixedLengthTypeTrait(8);

        if (isQuantized) {
            // Field 1: centroidId (raw int - 4 bytes, no type tag) — MUST stay at index 1
            outputRecFields[1] = IntegerSerializerDeserializer.INSTANCE;
            outputTypeTraits[1] = new FixedLengthTypeTrait(4);
            // Field 2: quantized_distance (raw double - 8 bytes, distance on quantized representations)
            outputRecFields[2] = DoubleSerializerDeserializer.INSTANCE;
            outputTypeTraits[2] = new FixedLengthTypeTrait(8);
            // Field 3: quantized_embedding (variable-length byte array)
            outputRecFields[3] = ByteArraySerializerDeserializer.INSTANCE;
            outputTypeTraits[3] = VarLengthTypeTrait.INSTANCE;
        } else {
            // Field 1: centroidId (raw int - 4 bytes, no type tag)
            outputRecFields[1] = IntegerSerializerDeserializer.INSTANCE;
            outputTypeTraits[1] = new FixedLengthTypeTrait(4);
        }

        // Add primary key fields
        for (int i = 0; i < numPrimaryKeys; i++) {
            int secondaryRecIdx = numSecondaryKeys + i;
            outputRecFields[numOutputSecondaryFields + i] = secondaryRecDesc.getFields()[secondaryRecIdx];
            outputTypeTraits[numOutputSecondaryFields + i] = secondaryRecDesc.getTypeTraits()[secondaryRecIdx];
        }

        // Add include fields after PKs
        for (int i = 0; i < numIncludeFieldsForOutput; i++) {
            int secondaryRecIdx = 1 + i; // Skip embedding at index 0
            outputRecFields[numOutputSecondaryFields + numPrimaryKeys + i] =
                    secondaryRecDesc.getFields()[secondaryRecIdx];
            outputTypeTraits[numOutputSecondaryFields + numPrimaryKeys + i] =
                    secondaryRecDesc.getTypeTraits()[secondaryRecIdx];
        }

        RecordDescriptor outputRecDesc = new RecordDescriptor(outputRecFields, outputTypeTraits);
        assertBulkLoadOutputLayout(outputRecDesc, isQuantized, numPrimaryKeys, numIncludeFieldsForOutput);

        // Partitioning properties for compute-storage map (used by bulk loader and later by LSMIndexBulkLoad)
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);

        // Create VTreeBulkLoaderAndGroupingOperatorDescriptor
        IScalarEvaluatorFactory vectorFieldAccessor = new ColumnAccessEvalFactory(0);

        // Calculate number of include fields for field reordering in createTransformedTuple
        int numIncludeFieldsForBulkLoader =
                (indexDetails.getIncludeFieldNames() != null) ? indexDetails.getIncludeFieldNames().size() : 0;

        VTreeBulkLoaderAndGroupingOperatorDescriptor bulkLoaderAndGroupingOp =
                new VTreeBulkLoaderAndGroupingOperatorDescriptor(spec, dataflowHelperFactory, secondaryRecDesc,
                        outputRecDesc, vectorFieldAccessor, distanceMetric, vectorDimension, numPrimaryKeys,
                        numIncludeFieldsForBulkLoader, isQuantized, partitioningProperties.getComputeStorageMap(),
                        levelwiseEpsilon, crossPollinationM, rngFactor);
        bulkLoaderAndGroupingOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, bulkLoaderAndGroupingOp,
                primaryPartitionConstraint);

        // Connect CastAssign → BulkLoaderAndGrouping (which now outputs transformed tuples)
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, bulkLoaderAndGroupingOp, 0);

        sourceOp = bulkLoaderAndGroupingOp;

        // ExternalSortOperatorDescriptor - Sort by [centroidId, distance]
        // Record descriptor for the sort is the same outputRecDesc built above; it already depends on quantization:
        // - Non-quantized: outputRecDesc has 2 secondary fields (distance, centroidId) then PKs, includes.
        // - Quantized:    outputRecDesc has 4 secondary fields (distance, centroidId, quantized_distance,
        //                 quantized_embedding) then PKs, includes.
        // Sort keys are always field 1 (centroidId) and field 0 (distance), so no branch on isQuantized is needed.
        int[] sortFields = { 1, 0 };
        assert sortFields[0] == 1 && sortFields[1] == 0;
        IBinaryComparatorFactory[] sortComparatorFactories = { IntegerBinaryComparatorFactory.INSTANCE, // centroidId (raw int)
                DoubleBinaryComparatorFactory.INSTANCE // distance (raw double)
        };
        // Ensure minimum frames for sort operator (must be > 1)
        int sortFrames = Math.max(sortNumFrames, 2);
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec, sortFrames, sortFields,
                sortComparatorFactories, outputRecDesc);
        sortOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        targetOp = sortOp;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        sourceOp = targetOp;

        // LSMIndexBulkLoadOperatorDescriptor - Load sorted tuples into VCTree index
        int[] fieldPermutation = createFieldPermutationForSortedDataBulkLoad(outputRecDesc);

        // Create primary key fields array for partitioner
        // Use numOutputSecondaryFields (2 or 4) not numSecondaryKeys (input secondary key count)
        // because the output record has distance+centroidId (and optionally qDist+qEmbed) before PKs
        int[] pkFields = createPkFieldsForBulkLoadOp(fieldPermutation, numOutputSecondaryFields);

        // Primary index helper factory: opened by the bulk loader (CREATE_INDEX usage below) so the loaded
        // component's LSMComponentId is derived from the primary's component-id range. Without this coupling
        // the component (and its checkpoint lastComponentId) defaults to (0,0) and disagrees with the primary,
        // causing local recovery to roll the whole partition back on restart.
        IndexDataflowHelperFactory primaryIndexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), primaryFileSplitProvider);

        // Create partitioner factory using primary key hash functions
        IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(metadataProvider);
        ITuplePartitionerFactory partitionerFactory = new FieldHashPartitionerFactory(pkFields, pkHashFunFactories,
                partitioningProperties.getNumberOfPartitions());

        // Create LSMIndexBulkLoadOperatorDescriptor for data loading
        LSMIndexBulkLoadOperatorDescriptor sortedBulkLoaderOp =
                new LSMIndexBulkLoadOperatorDescriptor(spec, outputRecDesc, // bulk-load pipeline layout (distance, centroidId, [qDist, qEmbed], PKs, includes)
                        fieldPermutation, 0.7f, // fillFactor
                        false, // verifyInput
                        numElementsHint, // numElementsHint
                        false, // checkIfEmptyIndex
                        dataflowHelperFactory, primaryIndexHelperFactory, BulkLoadUsage.CREATE_INDEX,
                        dataset.getDatasetId(), null, // tupleFilterFactory
                        partitionerFactory, partitioningProperties.getComputeStorageMap());
        sortedBulkLoaderOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortedBulkLoaderOp,
                primaryPartitionConstraint);
        targetOp = sortedBulkLoaderOp;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        sourceOp = targetOp;

        // Final sink: RecordDescriptor is secondaryRecDesc (assign-op layout); bulk load/sort use outputRecDesc
        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor sinkOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { sinkRuntimeFactory }, new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sinkOp, primaryPartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, sinkOp, 0);

        spec.addRoot(sinkOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        return spec;
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AlgebricksException {
        Index.VectorIndexDetails vectorIndexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        AdmObjectNode withObjectNode = vectorIndexDetails.getWithObjectNode();
        if (withObjectNode == null) {
            return super.buildCreationJobSpec();
        }

        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        int vectorDimensions = requireDimension(withObjectNode);

        // 1. Identify "ANALYZE" sample index
        Index sampleIndex = metadataProvider.findSampleIndex(dataset.getDatabaseName(), dataset.getDataverseName(),
                dataset.getDatasetName());
        if (sampleIndex == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Run ANALYZE on the dataset before creating a vector index.");
        }

        // 2. Extract quantization parameters (default label SQ8 matches DDL; bits 8 for SQ8)
        String qLabel = resolveEffectiveQuantizationLabel(withObjectNode.getOptionalString("quantization", null));
        int bits = bitsForQuantizationLabel(qLabel);
        // confidence_interval is not a supported WITH field (not in ALLOWED_VECTOR_INDEX_WITH_FIELDS), so the
        // WITH read always returned the default; use the constant directly until it becomes a real DDL knob.
        float confidenceInterval = DEFAULT_CONFIDENCE_INTERVAL;

        // 3. Prepare Vector Extraction
        List<List<String>> vectorFieldPath = vectorIndexDetails.getKeyFieldNames();
        if (vectorFieldPath == null || vectorFieldPath.isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Vector index must specify vector field");
        }
        List<String> vectorFieldName = vectorFieldPath.get(0);

        ARecordType itemType = (ARecordType) metadataProvider.findType(dataset);
        ARecordType metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        itemType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(itemType, dataset);

        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int recordColumn = dataset.getDatasetType() == DatasetType.INTERNAL ? numPrimaryKeys : 0;

        // --- Step 1: Scan Sample Index ---
        // Setup partitioning and projector
        PartitioningProperties samplePartitioningProperties =
                metadataProvider.getPartitioningProperties(dataset, sampleIndex.getIndexName());
        AlgebricksPartitionConstraint samplePartitionConstraint = samplePartitioningProperties.getConstraints();
        IFileSplitProvider sampleFileSplitProvider = samplePartitioningProperties.getSplitsProvider();
        ITupleProjectorFactory projectorFactory = IndexUtil.createPrimaryIndexScanTupleProjectorFactory(
                dataset.getDatasetFormatInfo(), ALL_FIELDS_TYPE, itemType, metaType, dataset.getPrimaryKeys().size());

        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);

        // Refactored: Use BTreeSearchOperatorDescriptor on the persistent sample index
        IStorageManager storageMgr = metadataProvider.getStorageComponentProvider().getStorageManager();
        IIndexDataflowHelperFactory sampleIndexHelperFactory =
                new IndexDataflowHelperFactory(storageMgr, sampleFileSplitProvider);

        ITransactionSubsystemProvider txnSubsystemProvider = TransactionSubsystemProvider.INSTANCE;
        ISearchOperationCallbackFactory searchCallbackFactory = new PrimaryIndexInstantSearchOperationCallbackFactory(
                dataset.getDatasetId(), dataset.getPrimaryBloomFilterFields(), txnSubsystemProvider,
                IRecoveryManager.ResourceType.LSM_BTREE);

        IOperatorDescriptor targetOp = new BTreeSearchOperatorDescriptor(spec,
                dataset.getPrimaryRecordDescriptor(metadataProvider), null, null, true, true, sampleIndexHelperFactory,
                false, false, null, searchCallbackFactory, null, null, false, null, null, -1, false, null, null,
                projectorFactory, null, samplePartitioningProperties.getComputeStorageMap());

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, samplePartitionConstraint);

        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // --- Step 2: Extract Vector Features ---
        // Vector Accessor
        IScalarEvaluatorFactory vectorFieldEvalFactory = createFieldAccessor(itemType, recordColumn, vectorFieldName);

        // Flattened Descriptor
        IDataFormat format = metadataProvider.getDataFormat();
        ISerializerDeserializerProvider serdeProvider = format.getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = format.getTypeTraitProvider();
        ISerializerDeserializer<?>[] flattenedSerDes =
                new ISerializerDeserializer[] { serdeProvider.getSerializerDeserializer(ADOUBLE) };
        ITypeTraits[] flattenedTypeTraits = new ITypeTraits[] { typeTraitProvider.getTypeTrait(ADOUBLE) };
        RecordDescriptor flattenedRecordDesc = new RecordDescriptor(flattenedSerDes, flattenedTypeTraits);

        targetOp = new VectorComponentExtractorOperatorDescriptor(spec, vectorFieldEvalFactory,
                dataset.getPrimaryRecordDescriptor(metadataProvider), flattenedRecordDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, samplePartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // --- Step 3: Local Aggregate ---
        // Aggregate Output Descriptor
        ISerializerDeserializer<?>[] aggOutputSerDes =
                new ISerializerDeserializer[] { serdeProvider.getSerializerDeserializer(ABINARY) };
        ITypeTraits[] aggOutputTypeTraits = new ITypeTraits[] { typeTraitProvider.getTypeTrait(ABINARY) };
        RecordDescriptor aggOutputRecordDesc = new RecordDescriptor(aggOutputSerDes, aggOutputTypeTraits);

        // Aggregate Function
        IScalarEvaluatorFactory[] aggArgs = new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0) };
        QuantizationConstantsAggregateDescriptor aggDescriptor =
                (QuantizationConstantsAggregateDescriptor) QuantizationConstantsAggregateDescriptor.FACTORY
                        .createFunctionDescriptor();
        aggDescriptor.setImmutableStates(confidenceInterval, bits);
        IAggregateEvaluatorFactory localAggFactory = aggDescriptor.createAggregateEvaluatorFactory(aggArgs);

        // Grouping (Preclustered - GroupAll)
        int[] groupFields = new int[0];
        int framesLimit = OptimizationConfUtil.getGroupByNumFrames(
                metadataProvider.getApplicationContext().getCompilerProperties(), metadataProvider.getConfig(),
                sourceLoc, metadataProvider.getApplicationContext().getCompilerProperties().getFrameSize());
        AbstractAggregatorDescriptorFactory localAggFactoryDesc = new SimpleAlgebricksAccumulatingAggregatorFactory(
                new IAggregateEvaluatorFactory[] { localAggFactory }, groupFields);
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[0];

        targetOp = new PreclusteredGroupOperatorDescriptor(spec, groupFields, comparatorFactories, localAggFactoryDesc,
                aggOutputRecordDesc, true, framesLimit);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, samplePartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // --- Step 4: Global Aggregate ---
        IAggregateEvaluatorFactory globalAggFactory = aggDescriptor.createAggregateEvaluatorFactory(aggArgs);
        AbstractAggregatorDescriptorFactory globalAggFactoryDesc = new SimpleAlgebricksAccumulatingAggregatorFactory(
                new IAggregateEvaluatorFactory[] { globalAggFactory }, groupFields);

        targetOp = new PreclusteredGroupOperatorDescriptor(spec, groupFields, comparatorFactories, globalAggFactoryDesc,
                aggOutputRecordDesc, true, framesLimit);

        // Global aggregate runs on a single partition
        AlgebricksAbsolutePartitionConstraint clusterLocations =
                metadataProvider.getApplicationContext().getClusterStateManager().getNodeSortedClusterLocations();
        AlgebricksAbsolutePartitionConstraint singlePartitionConstraint =
                new AlgebricksAbsolutePartitionConstraint(new String[] { clusterLocations.getLocations()[0] });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, singlePartitionConstraint);

        // Connect Local -> Global (OnePartition)
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, new OnePartitionComputerFactory()), sourceOp, 0,
                targetOp, 0);
        sourceOp = targetOp; // This is the Global Aggregate Output

        // --- Step 5: Quantized Index Create ---
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        int[][] computeStorageMap = partitioningProperties.getComputeStorageMap();
        // Build QuantizedIndexBuilderFactory (not the plain IndexBuilderFactory) so each partition's builder
        // carries the sampled quantization constants into the resource at build time. Mirrors
        // DatasetUtil.getIndexBuilderFactories, differing only in the concrete builder-factory type.
        QuantizedIndexBuilderFactory[][] indexBuilderFactories =
                new QuantizedIndexBuilderFactory[computeStorageMap.length][];
        for (int i = 0; i < computeStorageMap.length; i++) {
            int len = computeStorageMap[i].length;
            indexBuilderFactories[i] = new QuantizedIndexBuilderFactory[len];
            for (int k = 0; k < len; k++) {
                IResourceFactory resourceFactory = dataset.getResourceFactory(metadataProvider, index, itemType,
                        metaType, mergePolicyFactory, mergePolicyProperties);
                indexBuilderFactories[i][k] = new QuantizedIndexBuilderFactory(
                        metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider,
                        resourceFactory, true);
            }
        }

        QuantizedIndexCreateOperatorDescriptor createOp = new QuantizedIndexCreateOperatorDescriptor(spec,
                indexBuilderFactories, computeStorageMap, aggOutputRecordDesc);

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, createOp,
                secondaryPartitionConstraint);

        // Connect Broadcast: GlobalAgg -> Broadcast -> QuantizedCreate
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), sourceOp, 0, createOp, 0);

        spec.addRoot(createOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        return spec;
    }

    @Override
    protected int getNumSecondaryKeys() {
        Index.VectorIndexDetails vectorIndexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        // For vector indexes, we always have at least 1 secondary key (the vector field itself)
        // Include fields are additional fields beyond the vector field
        List<List<String>> includeFieldNames = vectorIndexDetails.getIncludeFieldNames();
        int includeFieldsCount = (includeFieldNames == null) ? 0 : includeFieldNames.size();
        return 1 + includeFieldsCount; // 1 for vector field + include fields
    }

    /**
     * ======
     * |  SK  |             Bloom filter
     * ======
     * ====== ======
     * |  SK  |  PK  |      comparators, type traits
     * ====== ======
     * ====== ........
     * |  SK  | Filter |    field access evaluators
     * ====== ........
     * ====== ====== ........
     * |  SK  |  PK  | Filter |   record fields
     * ====== ====== ........
     * ====== ========= ........ ........
     * |  PK  | Payload |  Meta  | Filter | enforced record
     * ====== ========= ........ ........
     */
    @Override
    protected void setSecondaryRecDescAndComparators() throws AlgebricksException {
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        secondaryFieldAccessEvalFactories = new IScalarEvaluatorFactory[numSecondaryKeys + numFilterFields];
        secondaryComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        secondaryBloomFilterKeyFields = new int[numSecondaryKeys];
        ISerializerDeserializer[] secondaryRecFields =
                new ISerializerDeserializer[numSecondaryKeys + numPrimaryKeys + numFilterFields];
        ISerializerDeserializer[] enforcedRecFields =
                new ISerializerDeserializer[1 + numPrimaryKeys + (dataset.hasMetaPart() ? 1 : 0) + numFilterFields];
        ITypeTraits[] enforcedTypeTraits =
                new ITypeTraits[1 + numPrimaryKeys + (dataset.hasMetaPart() ? 1 : 0) + numFilterFields];
        secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadataProvider.getDataFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = metadataProvider.getDataFormat().getTypeTraitProvider();
        IBinaryComparatorFactoryProvider comparatorFactoryProvider =
                metadataProvider.getDataFormat().getBinaryComparatorFactoryProvider();
        // Record column is 0 for external datasets, numPrimaryKeys for internal ones
        int recordColumn = dataset.getDatasetType() == DatasetType.INTERNAL ? numPrimaryKeys : 0;
        boolean isOverridingKeyFieldTypes = indexDetails.isOverridingKeyFieldTypes();

        // For VECTOR indexes, we process the vector field first, then include fields
        List<List<String>> keyFieldNames = indexDetails.getKeyFieldNames();
        List<List<String>> includeFieldNames = indexDetails.getIncludeFieldNames();
        List<IAType> includeFieldTypes = indexDetails.getIncludeFieldTypes();
        List<Integer> includeSourceIndicators = indexDetails.getIncludeFieldSourceIndicators();

        // Process the vector field as the first secondary key
        if (keyFieldNames != null && !keyFieldNames.isEmpty()) {
            // Vector field is always in the record part (source indicator 0)
            ARecordType sourceType = itemType;
            ARecordType enforcedType = enforcedItemType;
            int sourceColumn = recordColumn;

            List<String> vectorFieldName = keyFieldNames.get(0);

            // Try to get the actual field type from the schema first (for closed fields)
            IAType vectorFieldType = null;
            try {
                vectorFieldType = sourceType.getSubFieldType(vectorFieldName);
            } catch (AlgebricksException e) {
                // Field not found in schema, will use default for open fields
                vectorFieldType = null;
            }

            // If field type is not found in schema (open field), provide default type
            if (vectorFieldType == null) {
                vectorFieldType = DefaultOpenFieldType.getDefaultOpenFieldType(ARRAY);
            }

            Pair<IAType, Boolean> keyTypePair =
                    Index.getNonNullableOpenFieldType(index, vectorFieldType, vectorFieldName, sourceType);
            IAType keyType = keyTypePair.first;

            IScalarEvaluatorFactory vectorFieldAccessor =
                    createFieldAccessor(sourceType, sourceColumn, vectorFieldName);

            secondaryFieldAccessEvalFactories[0] =
                    createFieldCast(vectorFieldAccessor, isOverridingKeyFieldTypes, enforcedType, sourceType, keyType);
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            secondaryRecFields[0] = serdeProvider.getSerializerDeserializer(keyType);
            secondaryComparatorFactories[0] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
            secondaryTypeTraits[0] = typeTraitProvider.getTypeTrait(keyType);
            secondaryBloomFilterKeyFields[0] = 0;

        }

        // Process include fields (if any)
        if (includeFieldNames != null && !includeFieldNames.isEmpty() && includeFieldTypes != null
                && !includeFieldTypes.isEmpty() && includeFieldNames.size() == includeFieldTypes.size()) {
            for (int i = 0; i < includeFieldNames.size(); i++) {
                ARecordType sourceType;
                ARecordType enforcedType;
                int sourceColumn;
                if (includeSourceIndicators == null || includeSourceIndicators.get(i) == 0) {
                    sourceType = itemType;
                    sourceColumn = recordColumn;
                    enforcedType = enforcedItemType;
                } else {
                    sourceType = metaType;
                    sourceColumn = recordColumn + 1;
                    enforcedType = enforcedMetaType;
                }
                List<String> secFieldName = includeFieldNames.get(i);
                IAType secFieldType = null;

                // Safely get the field type, handling potential index out of bounds
                if (i < includeFieldTypes.size()) {
                    secFieldType = includeFieldTypes.get(i);
                }

                // Skip if the field type is null or if we couldn't get it
                if (secFieldType == null) {
                    continue;
                }

                // Include fields start at index 1 (index 0 is the vector field)
                int fieldIndex = 1 + i;
                Pair<IAType, Boolean> keyTypePair =
                        Index.getNonNullableOpenFieldType(index, secFieldType, secFieldName, sourceType);
                IAType keyType = keyTypePair.first;
                IScalarEvaluatorFactory secFieldAccessor = createFieldAccessor(sourceType, sourceColumn, secFieldName);
                secondaryFieldAccessEvalFactories[fieldIndex] =
                        createFieldCast(secFieldAccessor, isOverridingKeyFieldTypes, enforcedType, sourceType, keyType);
                anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
                // For nullable/missable include fields, use a NULL-safe serializer.
                // Unlike B-tree which filters out NULL records, vector INCLUDE fields preserve NULLs.
                IAType serdeType =
                        keyTypePair.second ? KeyFieldTypeUtil.makeUnknownableType(keyType, true, true) : keyType;
                secondaryRecFields[fieldIndex] = serdeProvider.getSerializerDeserializer(serdeType);
                secondaryComparatorFactories[fieldIndex] =
                        comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
                secondaryTypeTraits[fieldIndex] = typeTraitProvider.getTypeTrait(serdeType);
                secondaryBloomFilterKeyFields[fieldIndex] = fieldIndex;
            }
        }
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            // Add serializers and comparators for primary index fields.
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFields[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
                enforcedRecFields[i] = primaryRecDesc.getFields()[i];
                secondaryTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
                enforcedTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
                secondaryComparatorFactories[numSecondaryKeys + i] = primaryComparatorFactories[i];
            }
        } else {
            // Add serializers and comparators for RID fields.
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFields[numSecondaryKeys + i] = IndexingConstants.getSerializerDeserializer(i);
                enforcedRecFields[i] = IndexingConstants.getSerializerDeserializer(i);
                secondaryTypeTraits[numSecondaryKeys + i] = IndexingConstants.getTypeTraits(i);
                enforcedTypeTraits[i] = IndexingConstants.getTypeTraits(i);
                secondaryComparatorFactories[numSecondaryKeys + i] = IndexingConstants.getComparatorFactory(i);
            }
        }
        enforcedRecFields[numPrimaryKeys] = serdeProvider.getSerializerDeserializer(itemType);
        enforcedTypeTraits[numPrimaryKeys] = typeTraitProvider.getTypeTrait(itemType);
        if (dataset.hasMetaPart()) {
            enforcedRecFields[numPrimaryKeys + 1] = serdeProvider.getSerializerDeserializer(metaType);
            enforcedTypeTraits[numPrimaryKeys + 1] = typeTraitProvider.getTypeTrait(metaType);
        }

        if (numFilterFields > 0) {
            Integer filterSourceIndicator =
                    ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterSourceIndicator();
            ARecordType sourceType;
            ARecordType enforcedType;
            int sourceColumn;
            if (filterSourceIndicator == null || filterSourceIndicator == 0) {
                sourceType = itemType;
                sourceColumn = recordColumn;
                enforcedType = enforcedItemType;
            } else {
                sourceType = metaType;
                sourceColumn = recordColumn + 1;
                enforcedType = enforcedMetaType;
            }
            IAType filterType = Index.getNonNullableKeyFieldType(filterFieldName, sourceType).first;
            IScalarEvaluatorFactory filterAccessor = createFieldAccessor(sourceType, sourceColumn, filterFieldName);
            secondaryFieldAccessEvalFactories[numSecondaryKeys] =
                    createFieldCast(filterAccessor, isOverridingKeyFieldTypes, enforcedType, sourceType, filterType);
            ISerializerDeserializer serde = serdeProvider.getSerializerDeserializer(filterType);
            secondaryRecFields[numPrimaryKeys + numSecondaryKeys] = serde;
            enforcedRecFields[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)] = serde;
            enforcedTypeTraits[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)] =
                    typeTraitProvider.getTypeTrait(filterType);
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields, secondaryTypeTraits);
        enforcedRecDesc = new RecordDescriptor(enforcedRecFields, enforcedTypeTraits);

    }

    /**
     * Validates bulk-load output tuple layout (enabled with {@code -ea}).
     * centroidId must remain at index 1 for sort keys {@code {1, 0}}.
     */
    private static void assertBulkLoadOutputLayout(RecordDescriptor outputRecDesc, boolean isQuantized,
            int numPrimaryKeys, int numIncludeFieldsForOutput) {
        int numOutputSecondaryFields = isQuantized ? 4 : 2;
        int expectedFields = numOutputSecondaryFields + numPrimaryKeys + numIncludeFieldsForOutput;
        assert outputRecDesc.getFieldCount() == expectedFields;
        assert outputRecDesc.getTypeTraits()[0] instanceof FixedLengthTypeTrait
                && ((FixedLengthTypeTrait) outputRecDesc.getTypeTraits()[0]).getFixedLength() == 8;
        assert outputRecDesc.getTypeTraits()[1] instanceof FixedLengthTypeTrait
                && ((FixedLengthTypeTrait) outputRecDesc.getTypeTraits()[1]).getFixedLength() == 4;
        if (isQuantized) {
            assert outputRecDesc.getTypeTraits()[2] instanceof FixedLengthTypeTrait;
            assert outputRecDesc.getTypeTraits()[3] == VarLengthTypeTrait.INSTANCE;
        }
    }

    private int[] createFieldPermutationForBulkLoadOp(int numSecondaryKeyFields) {
        int[] fieldPermutation = new int[numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = i;
        }
        return fieldPermutation;
    }

    /**
     * Create field permutation for sorted data bulk load operation.
     * Creates identity permutation that includes all fields from outputRecDesc in the same order.
     * outputRecDesc format: [distance(0), centroidId(1), optional qDist(2) and qEmbed(3), pk..., includes...]
     *
     * @param outputRecDesc the output record descriptor containing all fields
     * @return fieldPermutation array mapping input fields to output fields in same order (identity)
     */
    private int[] createFieldPermutationForSortedDataBulkLoad(RecordDescriptor outputRecDesc) {
        int numFields = outputRecDesc.getFieldCount();
        int[] fieldPermutation = new int[numFields];
        // Identity permutation: include all fields in same order
        for (int i = 0; i < numFields; i++) {
            fieldPermutation[i] = i;
        }
        return fieldPermutation;
    }

    /**
     * Create primary key fields array for bulk load operation partitioner.
     * Primary keys start after all secondary fields in outputRecDesc:
     * Non-quantized: [distance(0), centroidId(1), pk..., include_fields...]
     * Quantized:     [distance(0), centroidId(1), qDist(2), qEmbed(3), pk..., include_fields...]
     *
     * @param fieldPermutation the field permutation array
     * @param numSecondaryKeys number of secondary key fields (2 for non-quantized, 4 for quantized)
     * @return array of field indices where primary keys are located
     */
    private int[] createPkFieldsForBulkLoadOp(int[] fieldPermutation, int numSecondaryKeys) {
        int[] pkFields = new int[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            pkFields[i] = fieldPermutation[numSecondaryKeys + i];
        }
        return pkFields;
    }
}
