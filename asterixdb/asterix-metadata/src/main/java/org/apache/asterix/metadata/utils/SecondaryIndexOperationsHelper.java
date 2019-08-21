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

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.external.operators.ExternalIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalIndexBulkModifyOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalScanOperatorDescriptor;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.lock.ExternalDatasetsRegistry;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.AndDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsUnknownDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NotDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

@SuppressWarnings("rawtypes")
// TODO: We should eventually have a hierarchy of classes that can create all
// possible index job specs,
// not just for creation.
public abstract class SecondaryIndexOperationsHelper {
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
    protected List<ExternalFile> externalFiles;
    protected int numPrimaryKeys;
    protected final SourceLocation sourceLoc;
    protected final int sortNumFrames;

    // Prevent public construction. Should be created via createIndexCreator().
    protected SecondaryIndexOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        this.dataset = dataset;
        this.index = index;
        this.metadataProvider = metadataProvider;
        this.itemType =
                (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        this.metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        Pair<ARecordType, ARecordType> enforcedTypes = getEnforcedType(index, itemType, metaType);
        this.enforcedItemType = enforcedTypes.first;
        this.enforcedMetaType = enforcedTypes.second;
        this.sourceLoc = sourceLoc;
        this.sortNumFrames = getSortNumFrames(metadataProvider, sourceLoc);
    }

    private static Pair<ARecordType, ARecordType> getEnforcedType(Index index, ARecordType aRecordType,
            ARecordType metaRecordType) throws AlgebricksException {
        return index.isOverridingKeyFieldTypes()
                ? TypeUtil.createEnforcedType(aRecordType, metaRecordType, Collections.singletonList(index))
                : new Pair<>(null, null);
    }

    private static int getSortNumFrames(MetadataProvider metadataProvider, SourceLocation sourceLoc)
            throws AlgebricksException {
        return OptimizationConfUtil.getSortNumFrames(metadataProvider.getApplicationContext().getCompilerProperties(),
                metadataProvider.getConfig(), sourceLoc);
    }

    public static SecondaryIndexOperationsHelper createIndexOperationsHelper(Dataset dataset, Index index,
            MetadataProvider metadataProvider, SourceLocation sourceLoc) throws AlgebricksException {

        SecondaryIndexOperationsHelper indexOperationsHelper;
        switch (index.getIndexType()) {
            case BTREE:
                indexOperationsHelper = new SecondaryBTreeOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            case RTREE:
                indexOperationsHelper = new SecondaryRTreeOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                indexOperationsHelper =
                        new SecondaryInvertedIndexOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, sourceLoc,
                        index.getIndexType());
        }
        indexOperationsHelper.init();
        return indexOperationsHelper;
    }

    public abstract JobSpecification buildCreationJobSpec() throws AlgebricksException;

    public abstract JobSpecification buildLoadingJobSpec() throws AlgebricksException;

    public abstract JobSpecification buildCompactJobSpec() throws AlgebricksException;

    public abstract JobSpecification buildDropJobSpec(Set<DropOption> options) throws AlgebricksException;

    protected abstract void setSecondaryRecDescAndComparators() throws AlgebricksException;

    protected abstract int getNumSecondaryKeys();

    protected void init() throws AlgebricksException {
        payloadSerde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
        metaSerde =
                metaType == null ? null : SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset, index.getIndexName());
        secondaryFileSplitProvider = secondarySplitsAndConstraint.first;
        secondaryPartitionConstraint = secondarySplitsAndConstraint.second;
        numPrimaryKeys = dataset.getPrimaryKeys().size();
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            filterFieldName = DatasetUtil.getFilterField(dataset);
            if (filterFieldName != null) {
                numFilterFields = 1;
            } else {
                numFilterFields = 0;
            }
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primarySplitsAndConstraint =
                    metadataProvider.getSplitProviderAndConstraints(dataset);
            primaryFileSplitProvider = primarySplitsAndConstraint.first;
            primaryPartitionConstraint = primarySplitsAndConstraint.second;
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
        secondaryBTreeFields = new int[index.getKeyFieldNames().size() + numPrimaryKeys];
        for (int i = 0; i < primaryBTreeFields.length; i++) {
            primaryBTreeFields[i] = i;
        }
        for (int i = 0; i < secondaryBTreeFields.length; i++) {
            secondaryBTreeFields[i] = i;
        }

        IAType type = itemType.getSubFieldType(filterFieldName);
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
        for (int i = 0; i < secondaryFieldAccessEvalFactories.length; ++i) {
            sefs[i] = secondaryFieldAccessEvalFactories[i];
        }
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

    IFunctionDescriptor createCastFunction(boolean strictCast, SourceLocation sourceLoc) throws AlgebricksException {
        IFunctionDescriptor castFuncDesc = metadataProvider.getFunctionManager()
                .lookupFunction(strictCast ? BuiltinFunctions.CAST_TYPE : BuiltinFunctions.CAST_TYPE_LAX, sourceLoc);
        castFuncDesc.setSourceLocation(sourceLoc);
        castFuncDesc.setImmutableStates(enforcedItemType, itemType);
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

    protected LSMIndexBulkLoadOperatorDescriptor createTreeIndexBulkLoadOp(JobSpecification spec,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor) {
        IndexDataflowHelperFactory primaryIndexDataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), primaryFileSplitProvider);
        // when an index is being created (not loaded) the filtration is introduced in the pipeline -> no tuple filter
        LSMIndexBulkLoadOperatorDescriptor treeIndexBulkLoadOp = new LSMIndexBulkLoadOperatorDescriptor(spec,
                secondaryRecDesc, fieldPermutation, fillFactor, false, numElementsHint, false, dataflowHelperFactory,
                primaryIndexDataflowHelperFactory, BulkLoadUsage.CREATE_INDEX, dataset.getDatasetId(), null);
        treeIndexBulkLoadOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return treeIndexBulkLoadOp;
    }

    protected TreeIndexBulkLoadOperatorDescriptor createExternalIndexBulkLoadOp(JobSpecification spec,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor) {
        ExternalIndexBulkLoadOperatorDescriptor treeIndexBulkLoadOp = new ExternalIndexBulkLoadOperatorDescriptor(spec,
                secondaryRecDesc, fieldPermutation, fillFactor, false, numElementsHint, false, dataflowHelperFactory,
                ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, metadataProvider), null);
        treeIndexBulkLoadOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return treeIndexBulkLoadOp;
    }

    public AlgebricksMetaOperatorDescriptor createFilterNullsSelectOp(JobSpecification spec, int numSecondaryKeyFields,
            RecordDescriptor secondaryRecDesc) {
        IScalarEvaluatorFactory[] andArgsEvalFactories = new IScalarEvaluatorFactory[numSecondaryKeyFields];
        NotDescriptor notDesc = new NotDescriptor();
        notDesc.setSourceLocation(sourceLoc);
        IsUnknownDescriptor isUnknownDesc = new IsUnknownDescriptor();
        isUnknownDesc.setSourceLocation(sourceLoc);
        for (int i = 0; i < numSecondaryKeyFields; i++) {
            // Access column i, and apply 'is not null'.
            ColumnAccessEvalFactory columnAccessEvalFactory = new ColumnAccessEvalFactory(i);
            IScalarEvaluatorFactory isUnknownEvalFactory =
                    isUnknownDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { columnAccessEvalFactory });
            IScalarEvaluatorFactory notEvalFactory =
                    notDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { isUnknownEvalFactory });
            andArgsEvalFactories[i] = notEvalFactory;
        }
        IScalarEvaluatorFactory selectCond;
        if (numSecondaryKeyFields > 1) {
            // Create conjunctive condition where all secondary index keys must
            // satisfy 'is not null'.
            AndDescriptor andDesc = new AndDescriptor();
            andDesc.setSourceLocation(sourceLoc);
            selectCond = andDesc.createEvaluatorFactory(andArgsEvalFactories);
        } else {
            selectCond = andArgsEvalFactories[0];
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

    // This method creates a source indexing operator for external data
    protected ExternalScanOperatorDescriptor createExternalIndexingOp(JobSpecification spec)
            throws AlgebricksException {
        // A record + primary keys
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[1 + numPrimaryKeys];
        ITypeTraits[] typeTraits = new ITypeTraits[1 + numPrimaryKeys];
        // payload serde and type traits for the record slot
        serdes[0] = payloadSerde;
        typeTraits[0] = TypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        //  serdes and type traits for rid fields
        for (int i = 1; i < serdes.length; i++) {
            serdes[i] = IndexingConstants.getSerializerDeserializer(i - 1);
            typeTraits[i] = IndexingConstants.getTypeTraits(i - 1);
        }
        // output record desc
        RecordDescriptor indexerDesc = new RecordDescriptor(serdes, typeTraits);

        // Create the operator and its partition constraits
        Pair<ExternalScanOperatorDescriptor, AlgebricksPartitionConstraint> indexingOpAndConstraints;
        try {
            indexingOpAndConstraints = ExternalIndexingOperations.createExternalIndexingOp(spec, metadataProvider,
                    dataset, itemType, indexerDesc, externalFiles, sourceLoc);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexingOpAndConstraints.first,
                indexingOpAndConstraints.second);

        // Set the primary partition constraints to this partition constraints
        primaryPartitionConstraint = indexingOpAndConstraints.second;
        return indexingOpAndConstraints.first;
    }

    protected AlgebricksMetaOperatorDescriptor createExternalAssignOp(JobSpecification spec, int numSecondaryKeys,
            RecordDescriptor secondaryRecDesc) {
        int[] outColumns = new int[numSecondaryKeys];
        int[] projectionList = new int[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            outColumns[i] = i + numPrimaryKeys + 1;
            projectionList[i] = i + numPrimaryKeys + 1;
        }

        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[secondaryFieldAccessEvalFactories.length];
        for (int i = 0; i < secondaryFieldAccessEvalFactories.length; ++i) {
            sefs[i] = secondaryFieldAccessEvalFactories[i];
        }
        //add External RIDs to the projection list
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[numSecondaryKeys + i] = i + 1;
        }

        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        return new AlgebricksMetaOperatorDescriptor(spec, 1, 1, new IPushRuntimeFactory[] { assign },
                new RecordDescriptor[] { secondaryRecDesc });
    }

    protected ExternalIndexBulkModifyOperatorDescriptor createExternalIndexBulkModifyOp(JobSpecification spec,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor) {
        // create a list of file ids
        int numOfDeletedFiles = 0;
        for (ExternalFile file : externalFiles) {
            if (file.getPendingOp() == ExternalFilePendingOp.DROP_OP) {
                numOfDeletedFiles++;
            }
        }
        int[] deletedFiles = new int[numOfDeletedFiles];
        int i = 0;
        for (ExternalFile file : externalFiles) {
            if (file.getPendingOp() == ExternalFilePendingOp.DROP_OP) {
                deletedFiles[i] = file.getFileNumber();
            }
        }
        ExternalIndexBulkModifyOperatorDescriptor treeIndexBulkLoadOp = new ExternalIndexBulkModifyOperatorDescriptor(
                spec, dataflowHelperFactory, deletedFiles, fieldPermutation, fillFactor, false, numElementsHint, null);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return treeIndexBulkLoadOp;
    }

    public List<ExternalFile> getExternalFiles() {
        return externalFiles;
    }

    public void setExternalFiles(List<ExternalFile> externalFiles) {
        this.externalFiles = externalFiles;
    }

    public RecordDescriptor getSecondaryRecDesc() {
        return secondaryRecDesc;
    }

    public IBinaryComparatorFactory[] getSecondaryComparatorFactories() {
        return secondaryComparatorFactories;
    }

    public IFileSplitProvider getSecondaryFileSplitProvider() {
        return secondaryFileSplitProvider;
    }
}
