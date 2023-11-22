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

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.aggregates.collections.FirstElementEvalFactory;
import org.apache.asterix.runtime.evaluators.comparisons.GreaterThanDescriptor;
import org.apache.asterix.runtime.operators.DatasetStreamStatsOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.runningaggregates.std.SampleSlotRunningAggregateFunctionFactory;
import org.apache.asterix.runtime.runningaggregates.std.TidRunningAggregateDescriptor;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggrun.RunningAggregateRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamProjectRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionerFactory;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.sort.SortGroupByOperatorDescriptor;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

/**
 * Utility class for sampling operations.
 * <p>
 * The sampling method described in:
 * "A Convenient Algorithm for Drawing a Simple Random Sample",
 * by A. I. McLeod and D. R. Bellhouse
 */
public class SampleOperationsHelper implements ISecondaryIndexOperationsHelper {

    public static final String DATASET_STATS_OPERATOR_NAME = "Sample.DatasetStats";

    private final MetadataProvider metadataProvider;
    private final Dataset dataset;
    private final Index sampleIdx;
    private final SourceLocation sourceLoc;

    private ARecordType itemType;
    private ARecordType metaType;
    private RecordDescriptor recordDesc;
    private IBinaryComparatorFactory[] comparatorFactories;
    private IFileSplitProvider fileSplitProvider;
    private AlgebricksPartitionConstraint partitionConstraint;
    private ILSMMergePolicyFactory mergePolicyFactory;
    private Map<String, String> mergePolicyProperties;
    private int groupbyNumFrames;
    private int[][] computeStorageMap;
    private int numPartitions;

    protected SampleOperationsHelper(Dataset dataset, Index sampleIdx, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) {
        this.dataset = dataset;
        this.sampleIdx = sampleIdx;
        this.metadataProvider = metadataProvider;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void init() throws AlgebricksException {
        itemType = (ARecordType) metadataProvider.findType(dataset.getItemTypeDatabaseName(),
                dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        itemType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(itemType, metaType, dataset);

        recordDesc = dataset.getPrimaryRecordDescriptor(metadataProvider);
        comparatorFactories = dataset.getPrimaryComparatorFactories(metadataProvider, itemType, metaType);
        groupbyNumFrames = getGroupByNumFrames(metadataProvider, sourceLoc);

        // make sure to always use the dataset + index to get the partitioning properties
        // this is because in some situations the nodegroup of the passed dataset is different from the index
        // this can happen during a rebalance for example where the dataset represents the new target dataset while
        // the index object information is fetched from the old source dataset
        PartitioningProperties samplePartitioningProperties =
                metadataProvider.getPartitioningProperties(dataset, sampleIdx.getIndexName());
        fileSplitProvider = samplePartitioningProperties.getSplitsProvider();
        partitionConstraint = samplePartitioningProperties.getConstraints();
        computeStorageMap = samplePartitioningProperties.getComputeStorageMap();
        numPartitions = samplePartitioningProperties.getNumberOfPartitions();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        mergePolicyFactory = compactionInfo.first;
        mergePolicyProperties = compactionInfo.second;
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IndexBuilderFactory[][] indexBuilderFactories =
                DatasetUtil.getIndexBuilderFactories(dataset, metadataProvider, sampleIdx, itemType, metaType,
                        fileSplitProvider, mergePolicyFactory, mergePolicyProperties, computeStorageMap);
        IndexCreateOperatorDescriptor indexCreateOp =
                new IndexCreateOperatorDescriptor(spec, indexBuilderFactories, computeStorageMap);
        indexCreateOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp, partitionConstraint);
        spec.addRoot(indexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        Index.SampleIndexDetails indexDetails = (Index.SampleIndexDetails) sampleIdx.getIndexDetails();
        int sampleCardinalityTarget = indexDetails.getSampleCardinalityTarget();
        long sampleSeed = indexDetails.getSampleSeed();
        IDataFormat format = metadataProvider.getDataFormat();
        int nFields = recordDesc.getFieldCount();
        int[] columns = new int[nFields];
        for (int i = 0; i < nFields; i++) {
            columns[i] = i;
        }
        IStorageManager storageMgr = metadataProvider.getStorageComponentProvider().getStorageManager();
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IIndexDataflowHelperFactory dataflowHelperFactory =
                new IndexDataflowHelperFactory(storageMgr, fileSplitProvider);

        // job spec:
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // if format == column. Bring the entire record as we are sampling
        ITupleProjectorFactory projectorFactory = IndexUtil.createPrimaryIndexScanTupleProjectorFactory(
                dataset.getDatasetFormatInfo(), ALL_FIELDS_TYPE, itemType, metaType, dataset.getPrimaryKeys().size());

        // dummy key provider ----> primary index scan
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp =
                DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // primary index scan ----> stream stats op
        List<Index> dsIndexes = metadataProvider.getSecondaryIndexes(dataset);
        IndexDataflowHelperFactory[] indexes = new IndexDataflowHelperFactory[dsIndexes.size()];
        String[] names = new String[dsIndexes.size()];
        for (int i = 0; i < indexes.length; i++) {
            Index idx = dsIndexes.get(i);
            PartitioningProperties idxPartitioningProps =
                    metadataProvider.getPartitioningProperties(dataset, idx.getIndexName());
            indexes[i] = new IndexDataflowHelperFactory(storageMgr, idxPartitioningProps.getSplitsProvider());
            names[i] = idx.getIndexName();
        }
        targetOp = new DatasetStreamStatsOperatorDescriptor(spec, recordDesc, DATASET_STATS_OPERATOR_NAME, indexes,
                names, computeStorageMap);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // stream stats op ----> (running agg + select)
        // ragg produces a slot number and a tuple counter for each tuple
        // If the slot number is 0 then the tuple is not in the sample and is removed by subsequent select op.
        // If the slot number is greater than 0 then the tuple is in the sample.
        // There could be several tuples with the same slot number, the latest one wins
        // (with the greatest tuple counter). This is accomplished by the group by below
        BuiltinType raggSlotType = BuiltinType.AINT32;
        BuiltinType raggCounterType = BuiltinType.AINT64;
        int[] raggProjectColumns = new int[nFields + 2];
        raggProjectColumns[0] = nFields;
        raggProjectColumns[1] = nFields + 1;
        System.arraycopy(columns, 0, raggProjectColumns, 2, nFields);
        int[] raggAggColumns = { nFields, nFields + 1 };

        ISerializerDeserializerProvider serdeProvider = format.getSerdeProvider();
        ISerializerDeserializer[] raggSerdes = new ISerializerDeserializer[nFields + 2];
        raggSerdes[0] = serdeProvider.getSerializerDeserializer(raggSlotType);
        raggSerdes[1] = serdeProvider.getSerializerDeserializer(raggCounterType);
        System.arraycopy(recordDesc.getFields(), 0, raggSerdes, 2, nFields);

        ITypeTraitProvider typeTraitProvider = format.getTypeTraitProvider();
        ITypeTraits[] raggTraits = new ITypeTraits[nFields + 2];
        raggTraits[0] = typeTraitProvider.getTypeTrait(raggSlotType);
        raggTraits[1] = typeTraitProvider.getTypeTrait(raggCounterType);
        System.arraycopy(recordDesc.getTypeTraits(), 0, raggTraits, 2, nFields);

        RecordDescriptor raggRecordDesc = new RecordDescriptor(raggSerdes, raggTraits);

        IRunningAggregateEvaluatorFactory raggSlotEvalFactory =
                new SampleSlotRunningAggregateFunctionFactory(sampleCardinalityTarget, sampleSeed);
        IRunningAggregateEvaluatorFactory raggCounterEvalFactory = TidRunningAggregateDescriptor.FACTORY
                .createFunctionDescriptor().createRunningAggregateEvaluatorFactory(new IScalarEvaluatorFactory[0]);
        RunningAggregateRuntimeFactory raggRuntimeFactory =
                new RunningAggregateRuntimeFactory(raggProjectColumns, raggAggColumns,
                        new IRunningAggregateEvaluatorFactory[] { raggSlotEvalFactory, raggCounterEvalFactory });

        IFunctionDescriptor gtDescriptor = GreaterThanDescriptor.FACTORY.createFunctionDescriptor();
        gtDescriptor.setImmutableStates(raggSlotType, raggSlotType);
        IScalarEvaluatorFactory gtFactory =
                gtDescriptor.createEvaluatorFactory(new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0),
                        format.getConstantEvalFactory(new AsterixConstantValue(new AInt32(0))) });
        StreamSelectRuntimeFactory selectRuntimeFactory = new StreamSelectRuntimeFactory(gtFactory, null,
                format.getBinaryBooleanInspectorFactory(), false, -1, null);

        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { raggRuntimeFactory, selectRuntimeFactory },
                new RecordDescriptor[] { raggRecordDesc, raggRecordDesc });
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // (running agg + select) ---> group-by
        int[] groupFields = new int[] { 0 }; // [slot]
        int[] sortFields = new int[] { 0, 1 }; // [slot, counter]
        OrderOperator.IOrder sortSlotOrder = OrderOperator.ASC_ORDER;
        OrderOperator.IOrder sortCounterOrder = OrderOperator.DESC_ORDER;
        IBinaryComparatorFactoryProvider comparatorFactoryProvider = format.getBinaryComparatorFactoryProvider();
        IBinaryComparatorFactory[] raggCmpFactories = {
                comparatorFactoryProvider.getBinaryComparatorFactory(raggSlotType,
                        sortSlotOrder.getKind() == OrderOperator.IOrder.OrderKind.ASC),
                comparatorFactoryProvider.getBinaryComparatorFactory(raggCounterType,
                        sortCounterOrder.getKind() == OrderOperator.IOrder.OrderKind.ASC) };

        INormalizedKeyComputerFactoryProvider normKeyProvider = format.getNormalizedKeyComputerFactoryProvider();
        INormalizedKeyComputerFactory[] normKeyFactories = {
                normKeyProvider.getNormalizedKeyComputerFactory(raggSlotType,
                        sortSlotOrder.getKind() == OrderOperator.IOrder.OrderKind.ASC),
                normKeyProvider.getNormalizedKeyComputerFactory(raggCounterType,
                        sortCounterOrder.getKind() == OrderOperator.IOrder.OrderKind.ASC) };

        // agg = [counter, .. original columns ..]
        IAggregateEvaluatorFactory[] aggFactories = new IAggregateEvaluatorFactory[nFields + 1];
        for (int i = 0; i < aggFactories.length; i++) {
            aggFactories[i] = new FirstElementEvalFactory(
                    new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(1 + i) }, false, sourceLoc);
        }
        AbstractAggregatorDescriptorFactory aggregatorFactory =
                new SimpleAlgebricksAccumulatingAggregatorFactory(aggFactories, groupFields);

        targetOp = new SortGroupByOperatorDescriptor(spec, groupbyNumFrames, sortFields, groupFields, normKeyFactories,
                raggCmpFactories, aggregatorFactory, aggregatorFactory, raggRecordDesc, raggRecordDesc, false);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // group by --> project (remove ragg fields)
        int[] projectColumns = new int[nFields];
        for (int i = 0; i < nFields; i++) {
            projectColumns[i] = 2 + i;
        }
        StreamProjectRuntimeFactory projectRuntimeFactory = new StreamProjectRuntimeFactory(projectColumns);
        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1, new IPushRuntimeFactory[] { projectRuntimeFactory },
                new RecordDescriptor[] { recordDesc });
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // project ---> bulk load op
        targetOp = createTreeIndexBulkLoadOp(spec, columns, dataflowHelperFactory,
                StorageConstants.DEFAULT_TREE_FILL_FACTOR, sampleCardinalityTarget);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // bulk load op ----> sink op
        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0, new IPushRuntimeFactory[] { sinkRuntimeFactory },
                new RecordDescriptor[] { recordDesc });
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        spec.addRoot(targetOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        return spec;
    }

    protected LSMIndexBulkLoadOperatorDescriptor createTreeIndexBulkLoadOp(JobSpecification spec,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor,
            long numElementHint) throws AlgebricksException {
        int[] pkFields = new int[dataset.getPrimaryKeys().size()];
        System.arraycopy(fieldPermutation, 0, pkFields, 0, pkFields.length);
        IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(metadataProvider);
        ITuplePartitionerFactory partitionerFactory =
                new FieldHashPartitionerFactory(pkFields, pkHashFunFactories, numPartitions);
        LSMIndexBulkLoadOperatorDescriptor treeIndexBulkLoadOp = new LSMIndexBulkLoadOperatorDescriptor(spec,
                recordDesc, fieldPermutation, fillFactor, false, numElementHint, true, dataflowHelperFactory, null,
                LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage.LOAD, dataset.getDatasetId(), null, partitionerFactory,
                computeStorageMap);
        treeIndexBulkLoadOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                partitionConstraint);
        return treeIndexBulkLoadOp;
    }

    @Override
    public JobSpecification buildDropJobSpec(Set<IndexDropOperatorDescriptor.DropOption> options)
            throws AlgebricksException {
        return SecondaryTreeIndexOperationsHelper.buildDropJobSpecImpl(dataset, sampleIdx, options, metadataProvider,
                sourceLoc);
    }

    @Override
    public JobSpecification buildCompactJobSpec() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IFileSplitProvider getSecondaryFileSplitProvider() {
        return fileSplitProvider;
    }

    @Override
    public RecordDescriptor getSecondaryRecDesc() {
        return recordDesc;
    }

    @Override
    public IBinaryComparatorFactory[] getSecondaryComparatorFactories() {
        return comparatorFactories;
    }

    @Override
    public AlgebricksPartitionConstraint getSecondaryPartitionConstraint() {
        return partitionConstraint;
    }

    private static int getGroupByNumFrames(MetadataProvider metadataProvider, SourceLocation sourceLoc)
            throws AlgebricksException {
        return OptimizationConfUtil.getGroupByNumFrames(
                metadataProvider.getApplicationContext().getCompilerProperties(), metadataProvider.getConfig(),
                sourceLoc);
    }
}
