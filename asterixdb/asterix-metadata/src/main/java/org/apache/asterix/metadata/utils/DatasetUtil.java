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

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.external.indexing.IndexingConstants;
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
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
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
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
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
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import org.apache.hyracks.storage.common.IResourceFactory;
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

    public static List<String> getFilterField(Dataset dataset) {
        return ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterField();
    }

    public static IBinaryComparatorFactory[] computeFilterBinaryComparatorFactories(Dataset dataset,
            ARecordType itemType, IBinaryComparatorFactoryProvider comparatorFactoryProvider)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }
        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
        IBinaryComparatorFactory[] bcfs = new IBinaryComparatorFactory[1];
        IAType type = itemType.getSubFieldType(filterField);
        bcfs[0] = comparatorFactoryProvider.getBinaryComparatorFactory(type, true);
        return bcfs;
    }

    public static ITypeTraits[] computeFilterTypeTraits(Dataset dataset, ARecordType itemType)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }
        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
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
        filterFields[0] = numKeys + 1;
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
     * @param keySourceIndicator indicates where the key is coming from, 1 from meta record, 0 from data record
     * @param keyIndex the key index we're checking the field against
     * @param fieldFromMeta whether the field is coming from the meta record or the data record
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
        CompactionPolicy compactionPolicy = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
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
        LOGGER.info("DROP DATASET: " + dataset);
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        }
        JobSpecification specPrimary = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset);
        IIndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), splitsAndConstraint.first);
        IndexDropOperatorDescriptor primaryBtreeDrop = new IndexDropOperatorDescriptor(specPrimary, indexHelperFactory);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specPrimary, primaryBtreeDrop,
                splitsAndConstraint.second);
        specPrimary.addRoot(primaryBtreeDrop);
        return specPrimary;
    }

    public static JobSpecification buildDropFilesIndexJobSpec(MetadataProvider metadataProvider, Dataset dataset)
            throws AlgebricksException {
        String indexName = IndexingConstants.getFilesIndexName(dataset.getDatasetName());
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset, indexName);
        IIndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), splitsAndConstraint.first);
        IndexDropOperatorDescriptor btreeDrop = new IndexDropOperatorDescriptor(spec, indexHelperFactory);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeDrop,
                splitsAndConstraint.second);
        spec.addRoot(btreeDrop);
        return spec;
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
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset);
        FileSplit[] fs = splitsAndConstraint.first.getFileSplits();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fs.length; i++) {
            sb.append(fs[i] + " ");
        }
        LOGGER.info("CREATING File Splits: " + sb.toString());
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        // prepare a LocalResourceMetadata which will be stored in NC's local resource
        // repository
        IResourceFactory resourceFactory = dataset.getResourceFactory(metadataProvider, index, itemType, metaItemType,
                compactionInfo.first, compactionInfo.second);
        IndexBuilderFactory indexBuilderFactory =
                new IndexBuilderFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
                        splitsAndConstraint.first, resourceFactory, true);
        IndexCreateOperatorDescriptor indexCreateOp = new IndexCreateOperatorDescriptor(spec, indexBuilderFactory);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp,
                splitsAndConstraint.second);
        spec.addRoot(indexCreateOp);
        return spec;
    }

    public static JobSpecification compactDatasetJobSpec(Dataverse dataverse, String datasetName,
            MetadataProvider metadataProvider) throws AlgebricksException {
        String dataverseName = dataverse.getDataverseName();
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset);
        IIndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), splitsAndConstraint.first);
        LSMTreeIndexCompactOperatorDescriptor compactOp =
                new LSMTreeIndexCompactOperatorDescriptor(spec, indexHelperFactory);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                splitsAndConstraint.second);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                splitsAndConstraint.second);
        spec.addRoot(compactOp);
        return spec;
    }

    /**
     * Creates a primary index scan operator for a given dataset.
     *
     * @param spec,
     *            the job specification.
     * @param metadataProvider,
     *            the metadata provider.
     * @param dataset,
     *            the dataset to scan.
     * @return a primary index scan operator.
     * @throws AlgebricksException
     */
    public static IOperatorDescriptor createPrimaryIndexScanOp(JobSpecification spec, MetadataProvider metadataProvider,
            Dataset dataset) throws AlgebricksException {
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primarySplitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset);
        IFileSplitProvider primaryFileSplitProvider = primarySplitsAndConstraint.first;
        AlgebricksPartitionConstraint primaryPartitionConstraint = primarySplitsAndConstraint.second;
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
                indexHelperFactory, false, false, null, searchCallbackFactory, null, null, false);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, primarySearchOp,
                primaryPartitionConstraint);
        return primarySearchOp;
    }

    /**
     * Creates a primary index upsert operator for a given dataset.
     *
     * @param spec,
     *            the job specification.
     * @param metadataProvider,
     *            the metadata provider.
     * @param dataset,
     *            the dataset to upsert.
     * @param inputRecordDesc,the
     *            record descriptor for an input tuple.
     * @param fieldPermutation,
     *            the field permutation according to the input.
     * @param missingWriterFactory,
     *            the factory for customizing missing value serialization.
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
        Index primaryIndex = metadataProvider.getIndex(dataset.getDataverseName(), dataset.getDatasetName(),
                dataset.getDatasetName());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset);

        // prepare callback
        int[] primaryKeyFields = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            primaryKeyFields[i] = i;
        }
        boolean hasSecondaries =
                metadataProvider.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName()).size() > 1;
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                storageComponentProvider, primaryIndex, IndexOperation.UPSERT, primaryKeyFields);
        ISearchOperationCallbackFactory searchCallbackFactory = dataset.getSearchCallbackFactory(
                storageComponentProvider, primaryIndex, IndexOperation.UPSERT, primaryKeyFields);
        IIndexDataflowHelperFactory idfh =
                new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(), splitsAndConstraint.first);
        LSMPrimaryUpsertOperatorDescriptor op;
        ITypeTraits[] outputTypeTraits = new ITypeTraits[inputRecordDesc.getFieldCount() + 1
                + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
        ISerializerDeserializer<?>[] outputSerDes = new ISerializerDeserializer[inputRecordDesc.getFieldCount() + 1
                + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
        IDataFormat dataFormat = metadataProvider.getDataFormat();

        int f = 0;
        // add the upsert indicator var
        outputSerDes[f] = dataFormat.getSerdeProvider().getSerializerDeserializer(BuiltinType.ABOOLEAN);
        outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(BuiltinType.ABOOLEAN);
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
        if (numFilterFields > 0) {
            String filterField = DatasetUtil.getFilterField(dataset).get(0);
            String[] fieldNames = itemType.getFieldNames();
            int i = 0;
            for (; i < fieldNames.length; i++) {
                if (fieldNames[i].equals(filterField)) {
                    break;
                }
            }
            fieldIdx = i;
            outputTypeTraits[f] = dataFormat.getTypeTraitProvider().getTypeTrait(itemType.getFieldTypes()[fieldIdx]);
            outputSerDes[f] =
                    dataFormat.getSerdeProvider().getSerializerDeserializer(itemType.getFieldTypes()[fieldIdx]);
            f++;
        }
        for (int j = 0; j < inputRecordDesc.getFieldCount(); j++) {
            outputTypeTraits[j + f] = inputRecordDesc.getTypeTraits()[j];
            outputSerDes[j + f] = inputRecordDesc.getFields()[j];
        }
        RecordDescriptor outputRecordDesc = new RecordDescriptor(outputSerDes, outputTypeTraits);
        op = new LSMPrimaryUpsertOperatorDescriptor(spec, outputRecordDesc, fieldPermutation, idfh,
                missingWriterFactory, modificationCallbackFactory, searchCallbackFactory,
                dataset.getFrameOpCallbackFactory(metadataProvider), numKeys, itemType, fieldIdx, hasSecondaries);
        return new Pair<>(op, splitsAndConstraint.second);
    }

    /**
     * Creates a dummy key provider operator for the primary index scan.
     *
     * @param spec,
     *            the job specification.
     * @param dataset,
     *            the dataset to scan.
     * @param metadataProvider,
     *            the metadata provider.
     * @return a dummy key provider operator.
     * @throws AlgebricksException
     */
    public static IOperatorDescriptor createDummyKeyProviderOp(JobSpecification spec, Dataset dataset,
            MetadataProvider metadataProvider) throws AlgebricksException {
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primarySplitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset);
        AlgebricksPartitionConstraint primaryPartitionConstraint = primarySplitsAndConstraint.second;

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
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, keyProviderOp,
                primaryPartitionConstraint);
        return keyProviderOp;
    }

    public static boolean isFullyQualifiedName(String datasetName) {
        return datasetName.indexOf('.') > 0; // NOSONAR a fully qualified name can't start with a .
    }

    public static String getFullyQualifiedName(Dataset dataset) {
        return dataset.getDataverseName() + '.' + dataset.getDatasetName();
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
    public static String createNodeGroupForNewDataset(String dataverseName, String datasetName, Set<String> ncNames,
            MetadataProvider metadataProvider) throws Exception {
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
    public static String createNodeGroupForNewDataset(String dataverseName, String datasetName, long rebalanceCount,
            Set<String> ncNames, MetadataProvider metadataProvider) throws Exception {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        String nodeGroup = dataverseName + "." + datasetName + (rebalanceCount == 0L ? "" : "_" + rebalanceCount);
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        appCtx.getMetadataLockManager().acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodeGroup);
        NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodeGroup);
        if (ng != null) {
            nodeGroup = nodeGroup + "_" + UUID.randomUUID().toString();
            appCtx.getMetadataLockManager().acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodeGroup);
        }
        MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, new NodeGroup(nodeGroup, new ArrayList<>(ncNames)));
        return nodeGroup;
    }

    // This doesn't work if the dataset  or the dataverse name contains a '.'
    public static Pair<String, String> getDatasetInfo(MetadataProvider metadata, String datasetArg) {
        String first;
        String second;
        int i = datasetArg.indexOf('.');
        if (i > 0 && i < datasetArg.length() - 1) {
            first = datasetArg.substring(0, i);
            second = datasetArg.substring(i + 1);
        } else {
            first = metadata.getDefaultDataverse() == null ? null : metadata.getDefaultDataverse().getDataverseName();
            second = datasetArg;
        }
        return new Pair<>(first, second);
    }
}
