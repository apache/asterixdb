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

import static org.apache.asterix.common.utils.IdentifierUtil.dataset;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.EMPTY_TYPE;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.visitor.SimpleStringBuilderForIATypeVisitor;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.asterix.runtime.operators.LSMPrimaryUpsertOperatorDescriptor;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionerFactory;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.DefaultTupleProjectorFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatasetUtil {
    private static final Logger LOGGER = LogManager.getLogger();
    /*
     * Dataset related operations
     */
    public static final byte OP_UPSERT = 0x03;

    private DatasetUtil() {
    }

    public static Integer getFilterSourceIndicator(Dataset dataset) {
        return ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterSourceIndicator();
    }

    public static List<String> getFilterField(Dataset dataset) {
        return ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterField();
    }

    public static IBinaryComparatorFactory[] computeFilterBinaryComparatorFactories(Dataset dataset,
            ARecordType recordType, ARecordType metaType, IBinaryComparatorFactoryProvider comparatorFactoryProvider)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }
        Integer filterFieldSourceIndicator = getFilterSourceIndicator(dataset);
        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
        IBinaryComparatorFactory[] bcfs = new IBinaryComparatorFactory[1];
        ARecordType itemType = filterFieldSourceIndicator == 0 ? recordType : metaType;
        IAType type = itemType.getSubFieldType(filterField);
        bcfs[0] = comparatorFactoryProvider.getBinaryComparatorFactory(type, true);
        return bcfs;
    }

    public static ITypeTraits[] computeFilterTypeTraits(Dataset dataset, ARecordType recordType, ARecordType metaType)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }
        Integer filterFieldSourceIndicator = getFilterSourceIndicator(dataset);
        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
        ARecordType itemType = filterFieldSourceIndicator == 0 ? recordType : metaType;
        ITypeTraits[] typeTraits = new ITypeTraits[1];
        IAType type = itemType.getSubFieldType(filterField);
        typeTraits[0] = TypeTraitProvider.INSTANCE.getTypeTrait(type);
        return typeTraits;
    }

    public static int[] createFilterFields(Dataset dataset) {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }

        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
        List<List<String>> partitioningKeys = dataset.getPrimaryKeys();
        int numKeys = partitioningKeys.size();

        int[] filterFields = new int[1];
        int valueFields = dataset.hasMetaPart() ? 2 : 1;
        filterFields[0] = numKeys + valueFields;
        return filterFields;
    }

    public static int[] createBTreeFieldsWhenThereisAFilter(Dataset dataset) {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }

        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }

        List<List<String>> partitioningKeys = dataset.getPrimaryKeys();
        int valueFields = dataset.hasMetaPart() ? 2 : 1;
        int[] btreeFields = new int[partitioningKeys.size() + valueFields];
        for (int i = 0; i < btreeFields.length; ++i) {
            btreeFields[i] = i;
        }
        return btreeFields;
    }

    /**
     * Returns the primary key source indicators of the {@code dataset} or {@code null} if the dataset does not have
     * primary key source indicators (e.g. external datasets)
     */
    public static List<Integer> getKeySourceIndicators(Dataset dataset) {
        IDatasetDetails datasetDetails = dataset.getDatasetDetails();
        if (datasetDetails.getDatasetType() == DatasetType.INTERNAL) {
            return ((InternalDatasetDetails) datasetDetails).getKeySourceIndicator();
        }
        return null;
    }

    public static int getPositionOfPartitioningKeyField(Dataset dataset, List<String> fieldExpr,
            boolean fieldFromMeta) {
        List<Integer> keySourceIndicator = null;
        IDatasetDetails datasetDetails = dataset.getDatasetDetails();
        if (datasetDetails.getDatasetType() == DatasetType.INTERNAL) {
            keySourceIndicator = ((InternalDatasetDetails) datasetDetails).getKeySourceIndicator();
        }
        List<List<String>> partitioningKeys = dataset.getPrimaryKeys();
        for (int i = 0; i < partitioningKeys.size(); i++) {
            List<String> partitioningKey = partitioningKeys.get(i);
            if (partitioningKey.equals(fieldExpr) && keySourceMatches(keySourceIndicator, i, fieldFromMeta)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Once it's determined that a field name is a key (by just comparing the names), this method checks whether the
     * field is actually a key by making sure the field is coming from the right record (data record or meta record),
     * e.g. if the field name happens to be equal to the key name but the field is coming from the data record while
     * the key is coming from the meta record.
     *
     * @param keySourceIndicator indicates where the key is coming from, 1 from meta record, 0 from data record
     * @param keyIndex           the key index we're checking the field against
     * @param fieldFromMeta      whether the field is coming from the meta record or the data record
     * @return true if the key source matches the field source. Otherwise, false.
     */
    private static boolean keySourceMatches(List<Integer> keySourceIndicator, int keyIndex, boolean fieldFromMeta) {
        if (keySourceIndicator != null) {
            return (fieldFromMeta && keySourceIndicator.get(keyIndex) == 1)
                    || (!fieldFromMeta && keySourceIndicator.get(keyIndex) == 0);
        }
        return true;
    }

    public static Pair<ILSMMergePolicyFactory, Map<String, String>> getMergePolicyFactory(Dataset dataset,
            MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        String policyName = dataset.getCompactionPolicy();
        CompactionPolicy compactionPolicy = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx, null,
                MetadataConstants.METADATA_DATAVERSE_NAME, policyName);
        String compactionPolicyFactoryClassName = compactionPolicy.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory;
        Map<String, String> properties = dataset.getCompactionPolicyProperties();
        try {
            mergePolicyFactory = (ILSMMergePolicyFactory) Class.forName(compactionPolicyFactoryClassName).newInstance();
            if (mergePolicyFactory.getName().compareTo(CorrelatedPrefixMergePolicyFactory.NAME) == 0) {
                properties.put(CorrelatedPrefixMergePolicyFactory.KEY_DATASET_ID,
                        Integer.toString(dataset.getDatasetId()));
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AlgebricksException(e);
        }
        return new Pair<>(mergePolicyFactory, properties);
    }

    public static void writePropertyTypeRecord(String name, String value, DataOutput out, ARecordType recordType)
            throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(recordType);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(0, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(1, fieldValue);

        propertyRecordBuilder.write(out, true);
    }

    public static ARecordType getMetaType(MetadataProvider metadataProvider, Dataset dataset)
            throws AlgebricksException {
        if (dataset.hasMetaPart()) {
            return (ARecordType) metadataProvider.findType(dataset.getMetaItemTypeDataverseName(),
                    dataset.getMetaItemTypeName());
        }
        return null;
    }

    public static JobSpecification dropDatasetJobSpec(Dataset dataset, MetadataProvider metadataProvider)
            throws AlgebricksException, ACIDException {
        return dropDatasetJobSpec(dataset, metadataProvider, Collections.emptySet());
    }

    public static JobSpecification dropDatasetJobSpec(Dataset dataset, MetadataProvider metadataProvider,
            Set<IndexDropOperatorDescriptor.DropOption> options) throws AlgebricksException, ACIDException {
        LOGGER.info("DROP DATASET: " + dataset);
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        }
        JobSpecification specPrimary = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        IIndexDataflowHelperFactory indexHelperFactory =
                new IndexDataflowHelperFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
                        partitioningProperties.getSplitsProvider());
        IndexDropOperatorDescriptor primaryBtreeDrop = new IndexDropOperatorDescriptor(specPrimary, indexHelperFactory,
                options, partitioningProperties.getComputeStorageMap());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specPrimary, primaryBtreeDrop,
                partitioningProperties.getConstraints());
        specPrimary.addRoot(primaryBtreeDrop);
        return specPrimary;
    }

    public static JobSpecification createDatasetJobSpec(Dataset dataset, MetadataProvider metadataProvider)
            throws AlgebricksException {
        Index index = IndexUtil.getPrimaryIndex(dataset);
        ARecordType itemType = (ARecordType) metadataProvider.findType(dataset);
        // get meta item type
        ARecordType metaItemType = null;
        if (dataset.hasMetaPart()) {
            metaItemType = (ARecordType) metadataProvider.findMetaType(dataset);
        }
        itemType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(itemType, metaItemType, dataset);

        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        FileSplit[] fs = partitioningProperties.getSplitsProvider().getFileSplits();
        StringBuilder sb = new StringBuilder();
        for (FileSplit f : fs) {
            sb.append(f).append(" ");
        }
        LOGGER.info("CREATING File Splits: {}", sb);
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        // prepare a LocalResourceMetadata which will be stored in NC's local resource
        // repository
        IResourceFactory resourceFactory = dataset.getResourceFactory(metadataProvider, index, itemType, metaItemType,
                compactionInfo.first, compactionInfo.second);
        IndexBuilderFactory indexBuilderFactory =
                new IndexBuilderFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
                        partitioningProperties.getSplitsProvider(), resourceFactory, true);
        IndexCreateOperatorDescriptor indexCreateOp = new IndexCreateOperatorDescriptor(spec, indexBuilderFactory,
                partitioningProperties.getComputeStorageMap());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp,
                partitioningProperties.getConstraints());
        spec.addRoot(indexCreateOp);
        return spec;
    }

    public static JobSpecification compactDatasetJobSpec(Dataverse dataverse, String datasetName,
            MetadataProvider metadataProvider) throws AlgebricksException {
        DataverseName dataverseName = dataverse.getDataverseName();
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, datasetName, dataverseName);
        }
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        IIndexDataflowHelperFactory indexHelperFactory =
                new IndexDataflowHelperFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
                        partitioningProperties.getSplitsProvider());
        LSMTreeIndexCompactOperatorDescriptor compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                indexHelperFactory, partitioningProperties.getComputeStorageMap());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                partitioningProperties.getConstraints());
        spec.addRoot(compactOp);
        return spec;
    }

    /**
     * Creates a primary index scan operator for a given dataset.
     *
     * @param spec,             the job specification.
     * @param metadataProvider, the metadata provider.
     * @param dataset,          the dataset to scan.
     * @return a primary index scan operator.
     * @throws AlgebricksException
     */
    public static IOperatorDescriptor createPrimaryIndexScanOp(JobSpecification spec, MetadataProvider metadataProvider,
            Dataset dataset) throws AlgebricksException {
        return createPrimaryIndexScanOp(spec, metadataProvider, dataset, DefaultTupleProjectorFactory.INSTANCE);
    }

    public static IOperatorDescriptor createPrimaryIndexScanOp(JobSpecification spec, MetadataProvider metadataProvider,
            Dataset dataset, ITupleProjectorFactory projectorFactory) throws AlgebricksException {
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        IFileSplitProvider primaryFileSplitProvider = partitioningProperties.getSplitsProvider();
        AlgebricksPartitionConstraint primaryPartitionConstraint = partitioningProperties.getConstraints();
        // -Infinity
        int[] lowKeyFields = null;
        // +Infinity
        int[] highKeyFields = null;
        ITransactionSubsystemProvider txnSubsystemProvider = TransactionSubsystemProvider.INSTANCE;
        ISearchOperationCallbackFactory searchCallbackFactory = new PrimaryIndexInstantSearchOperationCallbackFactory(
                dataset.getDatasetId(), dataset.getPrimaryBloomFilterFields(), txnSubsystemProvider,
                IRecoveryManager.ResourceType.LSM_BTREE);
        IndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), primaryFileSplitProvider);
        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec,
                dataset.getPrimaryRecordDescriptor(metadataProvider), lowKeyFields, highKeyFields, true, true,
                indexHelperFactory, false, false, null, searchCallbackFactory, null, null, false, null, null, -1, false,
                null, null, projectorFactory, null, partitioningProperties.getComputeStorageMap());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, primarySearchOp,
                primaryPartitionConstraint);
        return primarySearchOp;
    }

    /**
     * Creates a primary index upsert operator for a given dataset.
     *
     * @param spec,                 the job specification.
     * @param metadataProvider,     the metadata provider.
     * @param dataset,              the dataset to upsert.
     * @param inputRecordDesc,the   record descriptor for an input tuple.
     * @param fieldPermutation,     the field permutation according to the input.
     * @param missingWriterFactory, the factory for customizing missing value serialization.
     * @return a primary index scan operator and its location constraints.
     * @throws AlgebricksException
     */
    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> createPrimaryIndexUpsertOp(
            JobSpecification spec, MetadataProvider metadataProvider, Dataset dataset, RecordDescriptor inputRecordDesc,
            int[] fieldPermutation, IMissingWriterFactory missingWriterFactory) throws AlgebricksException {
        int numKeys = dataset.getPrimaryKeys().size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;
        ARecordType itemType = (ARecordType) metadataProvider.findType(dataset);
        ARecordType metaItemType = (ARecordType) metadataProvider.findMetaType(dataset);
        itemType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(itemType, metaItemType, dataset);

        Index primaryIndex = metadataProvider.getIndex(dataset.getDataverseName(), dataset.getDatasetName(),
                dataset.getDatasetName());
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);

        // prepare callback
        int[] primaryKeyFields = new int[numKeys];
        int[] pkFields = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            primaryKeyFields[i] = i;
            pkFields[i] = fieldPermutation[i];
        }
        boolean hasSecondaries =
                metadataProvider.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName()).size() > 1;
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                storageComponentProvider, primaryIndex, IndexOperation.UPSERT, primaryKeyFields);
        ISearchOperationCallbackFactory searchCallbackFactory = dataset.getSearchCallbackFactory(
                storageComponentProvider, primaryIndex, IndexOperation.UPSERT, primaryKeyFields);
        IIndexDataflowHelperFactory idfh = new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(),
                partitioningProperties.getSplitsProvider());
        LSMPrimaryUpsertOperatorDescriptor op;
        ITypeTraits[] outputTypeTraits = new ITypeTraits[inputRecordDesc.getFieldCount() + 1
                + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
        ISerializerDeserializer<?>[] outputSerDes = new ISerializerDeserializer[inputRecordDesc.getFieldCount() + 1
                + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
        IDataFormat dataFormat = metadataProvider.getDataFormat();

        int f = 0;
        // add the upsert operation var
        outputSerDes[f] = dataFormat.getSerdeProvider().getSerializerDeserializer(BuiltinType.AINT8);
        outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(BuiltinType.AINT8);
        f++;
        // add the previous record
        outputSerDes[f] = dataFormat.getSerdeProvider().getSerializerDeserializer(itemType);
        outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(itemType);
        f++;
        // add the previous meta second
        if (dataset.hasMetaPart()) {
            outputSerDes[f] = dataFormat.getSerdeProvider().getSerializerDeserializer(metaItemType);
            outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(metaItemType);
            f++;
        }
        // add the previous filter third
        int fieldIdx = -1;
        Integer filterSourceIndicator = null;
        ARecordType filterItemType = null;
        if (numFilterFields > 0) {
            filterSourceIndicator = DatasetUtil.getFilterSourceIndicator(dataset);
            String filterField = DatasetUtil.getFilterField(dataset).get(0);
            filterItemType = filterSourceIndicator == 0 ? itemType : metaItemType;
            String[] fieldNames = filterItemType.getFieldNames();
            int i = 0;
            for (; i < fieldNames.length; i++) {
                if (fieldNames[i].equals(filterField)) {
                    break;
                }
            }
            fieldIdx = i;
            outputTypeTraits[f] =
                    dataFormat.getTypeTraitProvider().getTypeTrait(filterItemType.getFieldTypes()[fieldIdx]);
            outputSerDes[f] =
                    dataFormat.getSerdeProvider().getSerializerDeserializer(filterItemType.getFieldTypes()[fieldIdx]);
            f++;
        }
        for (int j = 0; j < inputRecordDesc.getFieldCount(); j++) {
            outputTypeTraits[j + f] = inputRecordDesc.getTypeTraits()[j];
            outputSerDes[j + f] = inputRecordDesc.getFields()[j];
        }
        RecordDescriptor outputRecordDesc = new RecordDescriptor(outputSerDes, outputTypeTraits);

        // This allows to project only the indexed fields instead of the entirety of the record
        ARecordType requestedType = getPrevRecordType(metadataProvider, dataset, itemType);
        ITupleProjectorFactory projectorFactory = IndexUtil.createUpsertTupleProjectorFactory(
                dataset.getDatasetFormatInfo(), requestedType, itemType, metaItemType, numKeys);
        IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(metadataProvider);
        ITuplePartitionerFactory tuplePartitionerFactory = new FieldHashPartitionerFactory(pkFields, pkHashFunFactories,
                partitioningProperties.getNumberOfPartitions());
        op = new LSMPrimaryUpsertOperatorDescriptor(spec, outputRecordDesc, fieldPermutation, idfh,
                missingWriterFactory, modificationCallbackFactory, searchCallbackFactory,
                dataset.getFrameOpCallbackFactory(metadataProvider), numKeys, filterSourceIndicator, filterItemType,
                fieldIdx, hasSecondaries, projectorFactory, tuplePartitionerFactory,
                partitioningProperties.getComputeStorageMap());
        return new Pair<>(op, partitioningProperties.getConstraints());
    }

    /**
     * Returns a type that contains indexed fields for columnar datasets.
     * The type is used retrieve the previous record with only the indexed fields -- minimizing the
     * I/O cost for point lookups.
     *
     * @param metadataProvider metadata provider
     * @param dataset          the dataset to upsert to
     * @param itemType         dataset type
     * @return a type with the requested fields
     */
    private static ARecordType getPrevRecordType(MetadataProvider metadataProvider, Dataset dataset,
            ARecordType itemType) throws AlgebricksException {
        if (dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.ROW) {
            return itemType;
        }

        // Column
        List<Index> secondaryIndexes =
                metadataProvider.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());
        List<ARecordType> indexPaths = new ArrayList<>();

        for (Index index : secondaryIndexes) {
            if (!index.isSecondaryIndex() || index.isPrimaryKeyIndex() || index.isSampleIndex()) {
                continue;
            }

            if (index.getIndexType() == DatasetConfig.IndexType.BTREE) {
                Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                indexPaths.add(indexDetails.getIndexExpectedType());
            } else if (index.getIndexType() == DatasetConfig.IndexType.ARRAY) {
                Index.ArrayIndexDetails indexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
                indexPaths.add(indexDetails.getIndexExpectedType());
            }
        }

        ARecordType result = indexPaths.isEmpty() ? EMPTY_TYPE : ProjectionFiltrationTypeUtil.merge(indexPaths);

        if (LOGGER.isInfoEnabled() && result != EMPTY_TYPE) {
            SimpleStringBuilderForIATypeVisitor schemaPrinter = new SimpleStringBuilderForIATypeVisitor();
            StringBuilder builder = new StringBuilder();
            result.accept(schemaPrinter, builder);
            LOGGER.info("Upsert previous tuple schema: {}", LogRedactionUtil.userData(builder.toString()));
        }

        return result;
    }

    /**
     * Creates a dummy key provider operator for the primary index scan.
     *
     * @param spec,             the job specification.
     * @param dataset,          the dataset to scan.
     * @param metadataProvider, the metadata provider.
     * @return a dummy key provider operator.
     * @throws AlgebricksException
     */
    public static IOperatorDescriptor createDummyKeyProviderOp(JobSpecification spec, Dataset dataset,
            MetadataProvider metadataProvider) throws AlgebricksException {
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        AlgebricksPartitionConstraint primaryPartitionConstraint = partitioningProperties.getConstraints();
        IOperatorDescriptor dummyKeyProviderOp = createDummyKeyProviderOp(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, dummyKeyProviderOp,
                primaryPartitionConstraint);
        return dummyKeyProviderOp;
    }

    public static IOperatorDescriptor createCorrelatedDummyKeyProviderOp(JobSpecification spec,
            AlgebricksPartitionConstraint apc) throws AlgebricksException {
        IOperatorDescriptor dummyKeyProviderOp = createDummyKeyProviderOp(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, dummyKeyProviderOp, apc);
        return dummyKeyProviderOp;
    }

    private static IOperatorDescriptor createDummyKeyProviderOp(JobSpecification spec) throws AlgebricksException {
        // Build dummy tuple containing one field with a dummy value inside.
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        DataOutput dos = tb.getDataOutput();
        tb.reset();
        try {
            // Serialize dummy value into a field.
            IntegerSerializerDeserializer.INSTANCE.serialize(0, dos);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
        // Add dummy field.
        tb.addFieldEndOffset();
        ISerializerDeserializer[] keyRecDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);
        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        return keyProviderOp;
    }

    public static String getFullyQualifiedDisplayName(Dataset dataset) {
        return getFullyQualifiedDisplayName(dataset.getDataverseName(), dataset.getDatasetName());
    }

    public static String getFullyQualifiedDisplayName(DataverseName dataverseName, String datasetName) {
        return MetadataUtil.getFullyQualifiedDisplayName(dataverseName, datasetName);
    }

    /***
     * Creates a node group that is associated with a new dataset.
     *
     * @param dataverseName,
     *            the dataverse name of the dataset.
     * @param datasetName,
     *            the name of the dataset.
     * @param ncNames,
     *            the set of node names.
     * @param metadataProvider,
     *            the metadata provider.
     * @return the name of the created node group.
     * @throws Exception
     */
    public static String createNodeGroupForNewDataset(DataverseName dataverseName, String datasetName,
            Set<String> ncNames, MetadataProvider metadataProvider) throws Exception {
        return createNodeGroupForNewDataset(dataverseName, datasetName, 0L, ncNames, metadataProvider);
    }

    /***
     * Creates a node group that is associated with a new dataset.
     *
     * @param dataverseName,
     *            the dataverse name of the dataset.
     * @param datasetName,
     *            the name of the dataset.
     * @param rebalanceCount
     *            , the rebalance count of the dataset.
     * @param ncNames,
     *            the set of node names.
     * @param metadataProvider,
     *            the metadata provider.
     * @return the name of the created node group.
     * @throws Exception
     */
    public static String createNodeGroupForNewDataset(DataverseName dataverseName, String datasetName,
            long rebalanceCount, Set<String> ncNames, MetadataProvider metadataProvider) throws Exception {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        String nodeGroup = dataverseName.getCanonicalForm() + "." + datasetName
                + (rebalanceCount == 0L ? "" : "_" + rebalanceCount);
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        appCtx.getMetadataLockManager().acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodeGroup);
        NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodeGroup);
        if (ng != null) {
            nodeGroup = nodeGroup + "_" + UUID.randomUUID();
            appCtx.getMetadataLockManager().acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodeGroup);
        }
        MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, NodeGroup.createOrdered(nodeGroup, ncNames));
        return nodeGroup;
    }

    public static String getDatasetTypeDisplayName(DatasetType datasetType) {
        return datasetType == DatasetType.VIEW ? "view" : dataset();
    }

    public static boolean isNotView(Dataset dataset) {
        return dataset.getDatasetType() != DatasetType.VIEW;
    }

    public static boolean isFieldAccessPushdownSupported(Dataset dataset) {
        DatasetType datasetType = dataset.getDatasetType();
        if (datasetType == DatasetType.INTERNAL) {
            return dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.COLUMN;
        } else if (datasetType == DatasetType.EXTERNAL) {
            ExternalDatasetDetails edd = (ExternalDatasetDetails) dataset.getDatasetDetails();
            return ExternalDataUtils.supportsPushdown(edd.getProperties());
        }
        return false;
    }

    public static boolean isFilterPushdownSupported(Dataset dataset) {
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            return dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.COLUMN;
        }
        // External dataset support filter pushdown
        return true;
    }

    public static boolean isRangeFilterPushdownSupported(Dataset dataset) {
        return dataset.getDatasetType() == DatasetType.INTERNAL
                && dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.COLUMN;
    }
}
