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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AbstractFunctionDescriptor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.AndDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsUnknownDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NotDescriptor;
import org.apache.asterix.runtime.evaluators.functions.OrDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionerFactory;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

@SuppressWarnings("rawtypes")
// TODO: We should eventually have a hierarchy of classes that can create all
// possible index job specs,
// not just for creation.
public abstract class SecondaryIndexOperationsHelper implements ISecondaryIndexOperationsHelper {
    protected final MetadataProvider metadataProvider;
    protected final Dataset dataset;
    protected final Index index;
    protected final ARecordType itemType;
    protected final ARecordType metaType;
    protected final ARecordType enforcedItemType;
    protected final ARecordType enforcedMetaType;
    protected ISerializerDeserializer metaSerde;
    protected ISerializerDeserializer payloadSerde;
    protected IFileSplitProvider primaryFileSplitProvider;
    protected AlgebricksPartitionConstraint primaryPartitionConstraint;
    protected IFileSplitProvider secondaryFileSplitProvider;
    protected AlgebricksPartitionConstraint secondaryPartitionConstraint;
    protected boolean anySecondaryKeyIsNullable = false;
    protected long numElementsHint;
    protected IBinaryComparatorFactory[] primaryComparatorFactories;
    protected int[] primaryBloomFilterKeyFields;
    protected RecordDescriptor primaryRecDesc;
    protected IBinaryComparatorFactory[] secondaryComparatorFactories;
    protected ITypeTraits[] secondaryTypeTraits;
    protected int[] secondaryBloomFilterKeyFields;
    protected RecordDescriptor secondaryRecDesc;
    protected IScalarEvaluatorFactory[] secondaryFieldAccessEvalFactories;
    protected ILSMMergePolicyFactory mergePolicyFactory;
    protected Map<String, String> mergePolicyProperties;
    protected RecordDescriptor enforcedRecDesc;
    protected int numFilterFields;
    protected List<String> filterFieldName;
    protected ITypeTraits[] filterTypeTraits;
    protected IBinaryComparatorFactory[] filterCmpFactories;
    protected int[] secondaryFilterFields;
    protected int[] primaryFilterFields;
    protected int[] primaryBTreeFields;
    protected int[] secondaryBTreeFields;
    protected int numPrimaryKeys;
    protected final SourceLocation sourceLoc;
    protected final int sortNumFrames;

    // Prevent public construction. Should be created via createIndexCreator().
    protected SecondaryIndexOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        this.dataset = dataset;
        this.index = index;
        this.metadataProvider = metadataProvider;
        ARecordType recordType = (ARecordType) metadataProvider.findType(dataset.getItemTypeDatabaseName(),
                dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        this.metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        this.itemType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(recordType, metaType, dataset);

        Pair<ARecordType, ARecordType> enforcedTypes = getEnforcedType(index, itemType, metaType);
        this.enforcedItemType = enforcedTypes.first;
        this.enforcedMetaType = enforcedTypes.second;
        this.sourceLoc = sourceLoc;
        this.sortNumFrames = getSortNumFrames(metadataProvider, sourceLoc);
    }

    private static Pair<ARecordType, ARecordType> getEnforcedType(Index index, ARecordType aRecordType,
            ARecordType metaRecordType) throws AlgebricksException {
        return index.getIndexDetails().isOverridingKeyFieldTypes()
                ? TypeUtil.createEnforcedType(aRecordType, metaRecordType, Collections.singletonList(index))
                : new Pair<>(null, null);
    }

    private static int getSortNumFrames(MetadataProvider metadataProvider, SourceLocation sourceLoc)
            throws AlgebricksException {
        return OptimizationConfUtil.getSortNumFrames(metadataProvider.getApplicationContext().getCompilerProperties(),
                metadataProvider.getConfig(), sourceLoc);
    }

    public static ISecondaryIndexOperationsHelper createIndexOperationsHelper(Dataset dataset, Index index,
            MetadataProvider metadataProvider, SourceLocation sourceLoc) throws AlgebricksException {

        ISecondaryIndexOperationsHelper indexOperationsHelper;
        DatasetConfig.IndexType indexType = index.getIndexType();
        switch (indexType) {
            case ARRAY:
                indexOperationsHelper =
                        new SecondaryArrayIndexBTreeOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            case BTREE:
                indexOperationsHelper = new SecondaryBTreeOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            case RTREE:
                ensureNotColumnar(dataset, indexType, sourceLoc);
                indexOperationsHelper = new SecondaryRTreeOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                ensureNotColumnar(dataset, indexType, sourceLoc);
                indexOperationsHelper =
                        new SecondaryInvertedIndexOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            case SAMPLE:
                indexOperationsHelper = new SampleOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, sourceLoc,
                        index.getIndexType());
        }
        indexOperationsHelper.init();
        return indexOperationsHelper;
    }

    /**
     * Ensure only supported secondary indexes can be created against dataset
     * in {@link DatasetConfig.DatasetFormat#COLUMN} format
     *
     * @param dataset   primary index
     * @param indexType secondary index type
     * @param sourceLoc source location
     */
    private static void ensureNotColumnar(Dataset dataset, DatasetConfig.IndexType indexType, SourceLocation sourceLoc)
            throws AlgebricksException {
        if (dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.COLUMN) {
            throw CompilationException.create(ErrorCode.UNSUPPORTED_INDEX_IN_COLUMNAR_FORMAT, indexType, sourceLoc);
        }
    }

    @Override
    public abstract JobSpecification buildCreationJobSpec() throws AlgebricksException;

    @Override
    public abstract JobSpecification buildLoadingJobSpec() throws AlgebricksException;

    @Override
    public abstract JobSpecification buildCompactJobSpec() throws AlgebricksException;

    @Override
    public abstract JobSpecification buildDropJobSpec(Set<DropOption> options) throws AlgebricksException;

    protected abstract void setSecondaryRecDescAndComparators() throws AlgebricksException;

    protected abstract int getNumSecondaryKeys();

    @Override
    public void init() throws AlgebricksException {
        payloadSerde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
        metaSerde =
                metaType == null ? null : SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
        PartitioningProperties partitioningProperties;
        partitioningProperties =
                getSecondaryIndexBulkloadPartitioningProperties(metadataProvider, dataset, index.getIndexName());
        secondaryFileSplitProvider = partitioningProperties.getSplitsProvider();
        secondaryPartitionConstraint = partitioningProperties.getConstraints();
        numPrimaryKeys = dataset.getPrimaryKeys().size();
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            filterFieldName = DatasetUtil.getFilterField(dataset);
            if (filterFieldName != null) {
                numFilterFields = 1;
            } else {
                numFilterFields = 0;
            }
            PartitioningProperties datasetPartitioningProperties = getSecondaryIndexBulkloadPartitioningProperties(
                    metadataProvider, dataset, dataset.getDatasetName());
            primaryFileSplitProvider = datasetPartitioningProperties.getSplitsProvider();
            primaryPartitionConstraint = datasetPartitioningProperties.getConstraints();
            setPrimaryRecDescAndComparators();
        }
        setSecondaryRecDescAndComparators();
        numElementsHint = metadataProvider.getCardinalityPerPartitionHint(dataset);
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        mergePolicyFactory = compactionInfo.first;
        mergePolicyProperties = compactionInfo.second;
        if (numFilterFields > 0) {
            setFilterTypeTraitsAndComparators();
        }
    }

    private void setFilterTypeTraitsAndComparators() throws AlgebricksException {
        filterTypeTraits = new ITypeTraits[numFilterFields];
        filterCmpFactories = new IBinaryComparatorFactory[numFilterFields];
        secondaryFilterFields = new int[numFilterFields];
        primaryFilterFields = new int[numFilterFields];
        primaryBTreeFields = new int[numPrimaryKeys + 1];
        secondaryBTreeFields = new int[getNumSecondaryKeys() + numPrimaryKeys];
        for (int i = 0; i < primaryBTreeFields.length; i++) {
            primaryBTreeFields[i] = i;
        }
        for (int i = 0; i < secondaryBTreeFields.length; i++) {
            secondaryBTreeFields[i] = i;
        }

        IAType type = ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterSourceIndicator() == 0
                ? itemType.getSubFieldType(filterFieldName, itemType)
                : metaType.getSubFieldType(filterFieldName, metaType);
        filterCmpFactories[0] = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(type, true);
        filterTypeTraits[0] = TypeTraitProvider.INSTANCE.getTypeTrait(type);
        secondaryFilterFields[0] = getNumSecondaryKeys() + numPrimaryKeys;
        primaryFilterFields[0] = numPrimaryKeys + 1;
    }

    private void setPrimaryRecDescAndComparators() throws AlgebricksException {
        List<List<String>> partitioningKeys = dataset.getPrimaryKeys();
        ISerializerDeserializer[] primaryRecFields =
                new ISerializerDeserializer[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)];
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)];
        primaryComparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        primaryBloomFilterKeyFields = new int[numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadataProvider.getDataFormat().getSerdeProvider();
        List<Integer> indicators = null;
        if (dataset.hasMetaPart()) {
            indicators = ((InternalDatasetDetails) dataset.getDatasetDetails()).getKeySourceIndicator();
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            IAType keyType =
                    (indicators == null || indicators.get(i) == 0) ? itemType.getSubFieldType(partitioningKeys.get(i))
                            : metaType.getSubFieldType(partitioningKeys.get(i));
            primaryRecFields[i] = serdeProvider.getSerializerDeserializer(keyType);
            primaryComparatorFactories[i] =
                    BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType, true);
            primaryTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            primaryBloomFilterKeyFields[i] = i;
        }
        primaryRecFields[numPrimaryKeys] = payloadSerde;
        primaryTypeTraits[numPrimaryKeys] = TypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        if (dataset.hasMetaPart()) {
            primaryRecFields[numPrimaryKeys + 1] = payloadSerde;
            primaryTypeTraits[numPrimaryKeys + 1] = TypeTraitProvider.INSTANCE.getTypeTrait(metaType);
        }
        primaryRecDesc = new RecordDescriptor(primaryRecFields, primaryTypeTraits);
    }

    protected IScalarEvaluatorFactory createFieldAccessor(ARecordType recordType, int recordColumn,
            List<String> fieldName) throws AlgebricksException {
        IFunctionManager funManger = metadataProvider.getFunctionManager();
        IDataFormat dataFormat = metadataProvider.getDataFormat();
        return dataFormat.getFieldAccessEvaluatorFactory(funManger, recordType, fieldName, recordColumn, sourceLoc);
    }

    protected IScalarEvaluatorFactory createFieldCast(IScalarEvaluatorFactory fieldEvalFactory,
            boolean isOverridingKeyFieldTypes, IAType enforcedRecordType, ARecordType recordType, IAType targetType)
            throws AlgebricksException {
        IFunctionManager funManger = metadataProvider.getFunctionManager();
        IDataFormat dataFormat = metadataProvider.getDataFormat();

        // check IndexUtil.castDefaultNull(index), too, because we always want to cast even if the overriding type is
        // the same as the overridden type (this is for the case where overriding the type of closed field is allowed)
        // e.g. field "a" is a string in the dataset ds; CREATE INDEX .. ON ds(a:string) CAST (DEFAULT NULL)
        boolean castIndexedField = isOverridingKeyFieldTypes
                && (!enforcedRecordType.equals(recordType) || IndexUtil.castDefaultNull(index));
        if (!castIndexedField) {
            return fieldEvalFactory;
        }

        IScalarEvaluatorFactory castFieldEvalFactory;
        if (IndexUtil.castDefaultNull(index)) {
            castFieldEvalFactory = createConstructorFunction(funManger, dataFormat, fieldEvalFactory, targetType);
        } else if (index.isEnforced()) {
            IScalarEvaluatorFactory[] castArg = new IScalarEvaluatorFactory[] { fieldEvalFactory };
            castFieldEvalFactory =
                    createCastFunction(targetType, BuiltinType.ANY, true, sourceLoc).createEvaluatorFactory(castArg);
        } else {
            IScalarEvaluatorFactory[] castArg = new IScalarEvaluatorFactory[] { fieldEvalFactory };
            castFieldEvalFactory =
                    createCastFunction(targetType, BuiltinType.ANY, false, sourceLoc).createEvaluatorFactory(castArg);
        }
        return castFieldEvalFactory;
    }

    protected IScalarEvaluatorFactory createConstructorFunction(IFunctionManager funManager, IDataFormat dataFormat,
            IScalarEvaluatorFactory fieldEvalFactory, IAType fieldType) throws AlgebricksException {
        IAType targetType = TypeComputeUtils.getActualType(fieldType);
        Pair<FunctionIdentifier, IAObject> constructorWithFmt =
                IndexUtil.getTypeConstructorDefaultNull(index, targetType, sourceLoc);
        FunctionIdentifier typeConstructorFun = constructorWithFmt.first;
        IFunctionDescriptor typeConstructor = funManager.lookupFunction(typeConstructorFun, sourceLoc);
        IScalarEvaluatorFactory[] args;
        // add the format argument if specified
        if (constructorWithFmt.second != null) {
            IScalarEvaluatorFactory fmtEvalFactory =
                    dataFormat.getConstantEvalFactory(new AsterixConstantValue(constructorWithFmt.second));
            args = new IScalarEvaluatorFactory[] { fieldEvalFactory, fmtEvalFactory };
        } else {
            args = new IScalarEvaluatorFactory[] { fieldEvalFactory };
        }
        typeConstructor.setSourceLocation(sourceLoc);
        return typeConstructor.createEvaluatorFactory(args);
    }

    protected AlgebricksMetaOperatorDescriptor createAssignOp(JobSpecification spec, int numSecondaryKeyFields,
            RecordDescriptor secondaryRecDesc) throws AlgebricksException {
        int[] outColumns = new int[numSecondaryKeyFields + numFilterFields];
        int[] projectionList = new int[numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < numSecondaryKeyFields + numFilterFields; i++) {
            outColumns[i] = numPrimaryKeys + i;
        }
        int projCount = 0;
        for (int i = 0; i < numSecondaryKeyFields; i++) {
            projectionList[projCount++] = numPrimaryKeys + i;
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }
        if (numFilterFields > 0) {
            projectionList[projCount] = numPrimaryKeys + numSecondaryKeyFields;
        }

        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[secondaryFieldAccessEvalFactories.length];
        System.arraycopy(secondaryFieldAccessEvalFactories, 0, sefs, 0, secondaryFieldAccessEvalFactories.length);
        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        assign.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { secondaryRecDesc });
        asterixAssignOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                primaryPartitionConstraint);
        return asterixAssignOp;
    }

    protected AlgebricksMetaOperatorDescriptor createCastOp(JobSpecification spec, DatasetType dsType,
            boolean strictCast) throws AlgebricksException {
        int[] outColumns = new int[1];
        int[] projectionList = new int[(dataset.hasMetaPart() ? 2 : 1) + numPrimaryKeys];
        int recordIdx;
        //external datascan operator returns a record as the first field, instead of the last in internal case
        if (dsType == DatasetType.EXTERNAL) {
            recordIdx = 0;
            outColumns[0] = 0;
        } else {
            recordIdx = numPrimaryKeys;
            outColumns[0] = numPrimaryKeys;
        }
        for (int i = 0; i <= numPrimaryKeys; i++) {
            projectionList[i] = i;
        }
        if (dataset.hasMetaPart()) {
            projectionList[numPrimaryKeys + 1] = numPrimaryKeys + 1;
        }
        IScalarEvaluatorFactory[] castEvalFact =
                new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(recordIdx) };
        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[1];
        sefs[0] = createCastFunction(strictCast, sourceLoc).createEvaluatorFactory(castEvalFact);
        AssignRuntimeFactory castAssign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        castAssign.setSourceLocation(sourceLoc);
        return new AlgebricksMetaOperatorDescriptor(spec, 1, 1, new IPushRuntimeFactory[] { castAssign },
                new RecordDescriptor[] { enforcedRecDesc });
    }

    protected IFunctionDescriptor createCastFunction(boolean strictCast, SourceLocation sourceLoc)
            throws AlgebricksException {
        return createCastFunction(enforcedItemType, itemType, strictCast, sourceLoc);
    }

    protected IFunctionDescriptor createCastFunction(IAType targetType, IAType inputType, boolean strictCast,
            SourceLocation sourceLoc) throws AlgebricksException {
        IFunctionDescriptor castFuncDesc = metadataProvider.getFunctionManager()
                .lookupFunction(strictCast ? BuiltinFunctions.CAST_TYPE : BuiltinFunctions.CAST_TYPE_LAX, sourceLoc);
        castFuncDesc.setSourceLocation(sourceLoc);
        castFuncDesc.setImmutableStates(targetType, inputType);
        return castFuncDesc;
    }

    protected ExternalSortOperatorDescriptor createSortOp(JobSpecification spec,
            IBinaryComparatorFactory[] secondaryComparatorFactories, RecordDescriptor secondaryRecDesc) {
        int[] sortFields = new int[secondaryComparatorFactories.length];
        for (int i = 0; i < secondaryComparatorFactories.length; i++) {
            sortFields[i] = i;
        }
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec, sortNumFrames, sortFields,
                secondaryComparatorFactories, secondaryRecDesc);
        sortOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        return sortOp;
    }

    protected static boolean excludeUnknownKeys(Index index, Index.ValueIndexDetails details,
            boolean anySecKeyIsNullable) {
        return IndexUtil.excludesUnknowns(index) && (anySecKeyIsNullable || details.isOverridingKeyFieldTypes());
    }

    protected LSMIndexBulkLoadOperatorDescriptor createTreeIndexBulkLoadOp(JobSpecification spec,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor, int[] pkFields)
            throws AlgebricksException {
        IndexDataflowHelperFactory primaryIndexDataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), primaryFileSplitProvider);
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(metadataProvider);
        ITuplePartitionerFactory partitionerFactory = new FieldHashPartitionerFactory(pkFields, pkHashFunFactories,
                partitioningProperties.getNumberOfPartitions());
        // when an index is being created (not loaded) the filtration is introduced in the pipeline -> no tuple filter
        LSMIndexBulkLoadOperatorDescriptor treeIndexBulkLoadOp = new LSMIndexBulkLoadOperatorDescriptor(spec,
                secondaryRecDesc, fieldPermutation, fillFactor, false, numElementsHint, false, dataflowHelperFactory,
                primaryIndexDataflowHelperFactory, BulkLoadUsage.CREATE_INDEX, dataset.getDatasetId(), null,
                partitionerFactory, partitioningProperties.getComputeStorageMap());
        treeIndexBulkLoadOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return treeIndexBulkLoadOp;
    }

    public AlgebricksMetaOperatorDescriptor createFilterAllUnknownsSelectOp(JobSpecification spec,
            int numSecondaryKeyFields, RecordDescriptor secondaryRecDesc) throws AlgebricksException {
        return createFilterSelectOp(spec, numSecondaryKeyFields, secondaryRecDesc, OrDescriptor::new);
    }

    public AlgebricksMetaOperatorDescriptor createFilterAnyUnknownSelectOp(JobSpecification spec,
            int numSecondaryKeyFields, RecordDescriptor secondaryRecDesc) throws AlgebricksException {
        return createFilterSelectOp(spec, numSecondaryKeyFields, secondaryRecDesc, AndDescriptor::new);
    }

    public AlgebricksMetaOperatorDescriptor createCastFilterAnyUnknownSelectOp(JobSpecification spec,
            int numSecondaryKeyFields, RecordDescriptor secondaryRecDesc, List<IAType> castFieldTypes)
            throws AlgebricksException {
        return createFilterSelectOp(spec, numSecondaryKeyFields, secondaryRecDesc, AndDescriptor::new, castFieldTypes);
    }

    private AlgebricksMetaOperatorDescriptor createFilterSelectOp(JobSpecification spec, int numSecondaryKeyFields,
            RecordDescriptor secondaryRecDesc, Supplier<AbstractFunctionDescriptor> predicatesCombinerFuncSupplier)
            throws AlgebricksException {
        return createFilterSelectOp(spec, numSecondaryKeyFields, secondaryRecDesc, predicatesCombinerFuncSupplier,
                Collections.emptyList());
    }

    private AlgebricksMetaOperatorDescriptor createFilterSelectOp(JobSpecification spec, int numSecondaryKeyFields,
            RecordDescriptor secondaryRecDesc, Supplier<AbstractFunctionDescriptor> predicatesCombinerFuncSupplier,
            List<IAType> castFieldTypes) throws AlgebricksException {
        IScalarEvaluatorFactory[] predicateArgsEvalFactories = new IScalarEvaluatorFactory[numSecondaryKeyFields];
        NotDescriptor notDesc = new NotDescriptor();
        notDesc.setSourceLocation(sourceLoc);
        IsUnknownDescriptor isUnknownDesc = new IsUnknownDescriptor();
        isUnknownDesc.setSourceLocation(sourceLoc);
        for (int i = 0; i < numSecondaryKeyFields; i++) {
            // Access column i, and apply 'is not null'.
            ColumnAccessEvalFactory columnAccessEvalFactory = new ColumnAccessEvalFactory(i);
            IScalarEvaluatorFactory evalFactory = columnAccessEvalFactory;
            if (castFieldTypes != null && !castFieldTypes.isEmpty()) {
                IScalarEvaluatorFactory[] castArg = new IScalarEvaluatorFactory[] { columnAccessEvalFactory };
                evalFactory = createCastFunction(castFieldTypes.get(i), BuiltinType.ANY, index.isEnforced(), sourceLoc)
                        .createEvaluatorFactory(castArg);
            }
            IScalarEvaluatorFactory isUnknownEvalFactory =
                    isUnknownDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { evalFactory });
            IScalarEvaluatorFactory notEvalFactory =
                    notDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { isUnknownEvalFactory });
            predicateArgsEvalFactories[i] = notEvalFactory;
        }
        IScalarEvaluatorFactory selectCond;
        if (numSecondaryKeyFields > 1) {
            AbstractFunctionDescriptor predicatesCombiner = predicatesCombinerFuncSupplier.get();
            predicatesCombiner.setSourceLocation(sourceLoc);
            selectCond = predicatesCombiner.createEvaluatorFactory(predicateArgsEvalFactories);
        } else {
            selectCond = predicateArgsEvalFactories[0];
        }
        StreamSelectRuntimeFactory select =
                new StreamSelectRuntimeFactory(selectCond, null, BinaryBooleanInspector.FACTORY, false, -1, null);
        select.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor asterixSelectOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { select }, new RecordDescriptor[] { secondaryRecDesc });
        asterixSelectOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixSelectOp,
                primaryPartitionConstraint);
        return asterixSelectOp;
    }

    @Override
    public RecordDescriptor getSecondaryRecDesc() {
        return secondaryRecDesc;
    }

    @Override
    public IBinaryComparatorFactory[] getSecondaryComparatorFactories() {
        return secondaryComparatorFactories;
    }

    @Override
    public IFileSplitProvider getSecondaryFileSplitProvider() {
        return secondaryFileSplitProvider;
    }

    @Override
    public AlgebricksPartitionConstraint getSecondaryPartitionConstraint() {
        return secondaryPartitionConstraint;
    }

    private PartitioningProperties getSecondaryIndexBulkloadPartitioningProperties(MetadataProvider mp, Dataset dataset,
            String indexName) throws AlgebricksException {
        PartitioningProperties partitioningProperties = mp.getPartitioningProperties(dataset, indexName);
        // special case for bulkloading secondary indexes for datasets with correldated merge policy
        // to ensure correctness, we will run in as many locations as storage partitions
        // this will not be needed once ASTERIXDB-3176 is implemented
        if (this instanceof SecondaryCorrelatedTreeIndexOperationsHelper) {
            FileSplit[] fileSplits = partitioningProperties.getSplitsProvider().getFileSplits();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> sp =
                    StoragePathUtil.splitProviderAndPartitionConstraints(fileSplits);
            return PartitioningProperties.of(sp.getFirst(), sp.getSecond(),
                    DataPartitioningProvider.getOneToOnePartitionsMap(fileSplits.length));
        }
        return partitioningProperties;
    }
}
