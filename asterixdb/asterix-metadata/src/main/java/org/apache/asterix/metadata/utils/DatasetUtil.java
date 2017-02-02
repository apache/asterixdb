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
import java.io.File;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.transactions.IResourceFactory;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.BTreeDataflowHelperFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.utils.RuntimeComponentsProvider;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadataFactory;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import org.apache.hyracks.storage.common.file.LocalResource;

public class DatasetUtil {
    private static final Logger LOGGER = Logger.getLogger(DatasetUtil.class.getName());
    /*
     * Dataset related operations
     */
    public static final byte OP_READ = 0x00;
    public static final byte OP_INSERT = 0x01;
    public static final byte OP_DELETE = 0x02;
    public static final byte OP_UPSERT = 0x03;

    private DatasetUtil() {
    }

    public static IBinaryComparatorFactory[] computeKeysBinaryComparatorFactories(Dataset dataset,
            ARecordType itemType, ARecordType metaItemType, IBinaryComparatorFactoryProvider comparatorFactoryProvider)
            throws AlgebricksException {
        List<List<String>> partitioningKeys = getPartitioningKeys(dataset);
        IBinaryComparatorFactory[] bcfs = new IBinaryComparatorFactory[partitioningKeys.size()];
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            // Get comparators for RID fields.
            for (int i = 0; i < partitioningKeys.size(); i++) {
                try {
                    bcfs[i] = IndexingConstants.getComparatorFactory(i);
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }
        } else {
            InternalDatasetDetails dsd = (InternalDatasetDetails) dataset.getDatasetDetails();
            for (int i = 0; i < partitioningKeys.size(); i++) {
                IAType keyType = (dataset.hasMetaPart() && dsd.getKeySourceIndicator().get(i).intValue() == 1)
                        ? metaItemType.getSubFieldType(partitioningKeys.get(i))
                        : itemType.getSubFieldType(partitioningKeys.get(i));
                bcfs[i] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
            }
        }
        return bcfs;
    }

    public static int[] createBloomFilterKeyFields(Dataset dataset) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<List<String>> partitioningKeys = getPartitioningKeys(dataset);
        int[] bloomFilterKeyFields = new int[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); ++i) {
            bloomFilterKeyFields[i] = i;
        }
        return bloomFilterKeyFields;
    }

    public static IBinaryHashFunctionFactory[] computeKeysBinaryHashFunFactories(Dataset dataset, ARecordType itemType,
            IBinaryHashFunctionFactoryProvider hashFunProvider) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<List<String>> partitioningKeys = getPartitioningKeys(dataset);
        IBinaryHashFunctionFactory[] bhffs = new IBinaryHashFunctionFactory[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); i++) {
            IAType keyType = itemType.getSubFieldType(partitioningKeys.get(i));
            bhffs[i] = hashFunProvider.getBinaryHashFunctionFactory(keyType);
        }
        return bhffs;
    }

    public static ITypeTraits[] computeTupleTypeTraits(Dataset dataset, ARecordType itemType, ARecordType metaItemType)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<List<String>> partitioningKeys = DatasetUtil.getPartitioningKeys(dataset);
        int numKeys = partitioningKeys.size();
        ITypeTraits[] typeTraits;
        if (metaItemType != null) {
            typeTraits = new ITypeTraits[numKeys + 2];
            List<Integer> indicator = ((InternalDatasetDetails) dataset.getDatasetDetails()).getKeySourceIndicator();
            typeTraits[numKeys + 1] = TypeTraitProvider.INSTANCE.getTypeTrait(metaItemType);
            for (int i = 0; i < numKeys; i++) {
                IAType keyType;
                if (indicator.get(i) == 0) {
                    keyType = itemType.getSubFieldType(partitioningKeys.get(i));
                } else {
                    keyType = metaItemType.getSubFieldType(partitioningKeys.get(i));
                }
                typeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            }
        } else {
            typeTraits = new ITypeTraits[numKeys + 1];
            for (int i = 0; i < numKeys; i++) {
                IAType keyType;
                keyType = itemType.getSubFieldType(partitioningKeys.get(i));
                typeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            }
        }
        typeTraits[numKeys] = TypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        return typeTraits;
    }

    public static List<List<String>> getPartitioningKeys(Dataset dataset) {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return IndexingConstants
                    .getRIDKeys(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties());
        }
        return ((InternalDatasetDetails) dataset.getDatasetDetails()).getPartitioningKey();
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

    public static int[] createFilterFields(Dataset dataset) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }

        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
        List<List<String>> partitioningKeys = DatasetUtil.getPartitioningKeys(dataset);
        int numKeys = partitioningKeys.size();

        int[] filterFields = new int[1];
        filterFields[0] = numKeys + 1;
        return filterFields;
    }

    public static int[] createBTreeFieldsWhenThereisAFilter(Dataset dataset) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }

        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }

        List<List<String>> partitioningKeys = getPartitioningKeys(dataset);
        int valueFields = dataset.hasMetaPart() ? 2 : 1;
        int[] btreeFields = new int[partitioningKeys.size() + valueFields];
        for (int i = 0; i < btreeFields.length; ++i) {
            btreeFields[i] = i;
        }
        return btreeFields;
    }

    public static int getPositionOfPartitioningKeyField(Dataset dataset, String fieldExpr) {
        List<List<String>> partitioningKeys = DatasetUtil.getPartitioningKeys(dataset);
        for (int i = 0; i < partitioningKeys.size(); i++) {
            if ((partitioningKeys.get(i).size() == 1) && partitioningKeys.get(i).get(0).equals(fieldExpr)) {
                return i;
            }
        }
        return -1;
    }

    public static Pair<ILSMMergePolicyFactory, Map<String, String>> getMergePolicyFactory(Dataset dataset,
            MetadataTransactionContext mdTxnCtx) throws AlgebricksException, MetadataException {
        String policyName = dataset.getCompactionPolicy();
        CompactionPolicy compactionPolicy = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
                MetadataConstants.METADATA_DATAVERSE_NAME, policyName);
        String compactionPolicyFactoryClassName = compactionPolicy.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory;
        try {
            mergePolicyFactory =
                    (ILSMMergePolicyFactory) Class.forName(compactionPolicyFactoryClassName).newInstance();
            if (mergePolicyFactory.getName().compareTo("correlated-prefix") == 0) {
                ((CorrelatedPrefixMergePolicyFactory) mergePolicyFactory).setDatasetID(dataset.getDatasetId());
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AlgebricksException(e);
        }
        Map<String, String> properties = dataset.getCompactionPolicyProperties();
        return new Pair<>(mergePolicyFactory, properties);
    }

    @SuppressWarnings("unchecked")
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

    public static JobSpecification createDropDatasetJobSpec(Dataset dataset, Index primaryIndex,
            MetadataProvider metadataProvider)
            throws AlgebricksException, HyracksDataException, RemoteException, ACIDException {
        String datasetPath = dataset.getDataverseName() + File.separator + dataset.getDatasetName();
        LOGGER.info("DROP DATASETPATH: " + datasetPath);
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return RuntimeUtils.createJobSpecification();
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        ARecordType itemType =
                (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        ARecordType metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        JobSpecification specPrimary = RuntimeUtils.createJobSpecification();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset.getDataverseName(), dataset.getDatasetName(),
                        dataset.getDatasetName(), temp);
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        IIndexDataflowHelperFactory indexDataflowHelperFactory = dataset.getIndexDataflowHelperFactory(
                metadataProvider, primaryIndex, itemType, metaType, compactionInfo.first, compactionInfo.second);
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        IndexDropOperatorDescriptor primaryBtreeDrop =
                new IndexDropOperatorDescriptor(specPrimary, storageComponentProvider.getStorageManager(),
                        storageComponentProvider.getIndexLifecycleManagerProvider(), splitsAndConstraint.first,
                        indexDataflowHelperFactory, storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specPrimary, primaryBtreeDrop,
                splitsAndConstraint.second);
        specPrimary.addRoot(primaryBtreeDrop);
        return specPrimary;
    }

    public static JobSpecification buildDropFilesIndexJobSpec(MetadataProvider metadataProvider, Dataset dataset)
            throws AlgebricksException {
        String indexName = IndexingConstants.getFilesIndexName(dataset.getDatasetName());
        JobSpecification spec = RuntimeUtils.createJobSpecification();
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.splitProviderAndPartitionConstraintsForFilesIndex(dataset.getDataverseName(),
                        dataset.getDatasetName(), indexName, true);
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        String fileIndexName = BTreeDataflowHelperFactoryProvider.externalFileIndexName(dataset);
        Index fileIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                dataset.getDataverseName(), dataset.getDatasetName(), fileIndexName);
        IIndexDataflowHelperFactory dataflowHelperFactory = dataset.getIndexDataflowHelperFactory(metadataProvider,
                fileIndex, null, null, compactionInfo.first, compactionInfo.second);
        IndexDropOperatorDescriptor btreeDrop =
                new IndexDropOperatorDescriptor(spec, storageComponentProvider.getStorageManager(),
                        storageComponentProvider.getIndexLifecycleManagerProvider(), splitsAndConstraint.first,
                        dataflowHelperFactory, storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeDrop,
                splitsAndConstraint.second);
        spec.addRoot(btreeDrop);
        return spec;
    }

    public static JobSpecification dropDatasetJobSpec(Dataset dataset, Index primaryIndex,
            MetadataProvider metadataProvider)
            throws AlgebricksException, HyracksDataException, RemoteException, ACIDException {
        String datasetPath = dataset.getDataverseName() + File.separator + dataset.getDatasetName();
        LOGGER.info("DROP DATASETPATH: " + datasetPath);
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return RuntimeUtils.createJobSpecification();
        }

        boolean temp = dataset.getDatasetDetails().isTemp();
        ARecordType itemType =
                (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        ARecordType metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        JobSpecification specPrimary = RuntimeUtils.createJobSpecification();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset.getDataverseName(), dataset.getDatasetName(),
                        dataset.getDatasetName(), temp);
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());

        IIndexDataflowHelperFactory indexDataflowHelperFactory = dataset.getIndexDataflowHelperFactory(
                metadataProvider, primaryIndex, itemType, metaType, compactionInfo.first, compactionInfo.second);
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        IndexDropOperatorDescriptor primaryBtreeDrop =
                new IndexDropOperatorDescriptor(specPrimary, storageComponentProvider.getStorageManager(),
                        storageComponentProvider.getIndexLifecycleManagerProvider(), splitsAndConstraint.first,
                        indexDataflowHelperFactory, storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specPrimary, primaryBtreeDrop,
                splitsAndConstraint.second);

        specPrimary.addRoot(primaryBtreeDrop);

        return specPrimary;
    }

    public static JobSpecification createDatasetJobSpec(Dataverse dataverse, String datasetName,
            MetadataProvider metadataProvider) throws AsterixException, AlgebricksException {
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        String dataverseName = dataverse.getDataverseName();
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        Index index = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                datasetName, datasetName);
        boolean temp = dataset.getDatasetDetails().isTemp();
        ARecordType itemType =
                (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        // get meta item type
        ARecordType metaItemType = null;
        if (dataset.hasMetaPart()) {
            metaItemType = (ARecordType) metadataProvider.findType(dataset.getMetaItemTypeDataverseName(),
                    dataset.getMetaItemTypeName());
        }
        JobSpecification spec = RuntimeUtils.createJobSpecification();
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtil.computeKeysBinaryComparatorFactories(dataset,
                itemType, metaItemType, format.getBinaryComparatorFactoryProvider());
        ITypeTraits[] typeTraits = DatasetUtil.computeTupleTypeTraits(dataset, itemType, metaItemType);
        int[] bloomFilterKeyFields = DatasetUtil.createBloomFilterKeyFields(dataset);

        ITypeTraits[] filterTypeTraits = DatasetUtil.computeFilterTypeTraits(dataset, itemType);
        IBinaryComparatorFactory[] filterCmpFactories = DatasetUtil.computeFilterBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        int[] filterFields = DatasetUtil.createFilterFields(dataset);
        int[] btreeFields = DatasetUtil.createBTreeFieldsWhenThereisAFilter(dataset);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataverseName, datasetName, datasetName, temp);
        FileSplit[] fs = splitsAndConstraint.first.getFileSplits();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fs.length; i++) {
            sb.append(fs[i] + " ");
        }
        LOGGER.info("CREATING File Splits: " + sb.toString());

        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
        IResourceFactory localResourceMetadata = new LSMBTreeLocalResourceMetadataFactory(typeTraits,
                comparatorFactories, bloomFilterKeyFields, true, dataset.getDatasetId(), compactionInfo.first,
                compactionInfo.second, filterTypeTraits, filterCmpFactories, btreeFields, filterFields,
                dataset.getIndexOperationTrackerFactory(index), dataset.getIoOperationCallbackFactory(index),
                storageComponentProvider.getMetadataPageManagerFactory());
        ILocalResourceFactoryProvider localResourceFactoryProvider =
                new PersistentLocalResourceFactoryProvider(localResourceMetadata, LocalResource.LSMBTreeResource);
        IIndexDataflowHelperFactory dataflowHelperFactory = dataset.getIndexDataflowHelperFactory(metadataProvider,
                index, itemType, metaItemType, compactionInfo.first, compactionInfo.second);
        TreeIndexCreateOperatorDescriptor indexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                RuntimeComponentsProvider.RUNTIME_PROVIDER, RuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields,
                dataflowHelperFactory, localResourceFactoryProvider, NoOpOperationCallbackFactory.INSTANCE,
                storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp,
                splitsAndConstraint.second);
        spec.addRoot(indexCreateOp);
        return spec;
    }

    public static JobSpecification compactDatasetJobSpec(Dataverse dataverse, String datasetName,
            MetadataProvider metadataProvider) throws AsterixException, AlgebricksException {
        String dataverseName = dataverse.getDataverseName();
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        ARecordType itemType =
                (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        ARecordType metaItemType = DatasetUtil.getMetaType(metadataProvider, dataset);
        JobSpecification spec = RuntimeUtils.createJobSpecification();
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtil.computeKeysBinaryComparatorFactories(dataset,
                itemType, metaItemType, format.getBinaryComparatorFactoryProvider());
        ITypeTraits[] typeTraits = DatasetUtil.computeTupleTypeTraits(dataset, itemType, metaItemType);
        int[] blooFilterKeyFields = DatasetUtil.createBloomFilterKeyFields(dataset);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataverseName, datasetName, datasetName, temp);
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        Index index = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                dataset.getDataverseName(), datasetName, datasetName);
        IIndexDataflowHelperFactory dataflowHelperFactory = dataset.getIndexDataflowHelperFactory(metadataProvider,
                index, itemType, metaItemType, compactionInfo.first, compactionInfo.second);
        LSMTreeIndexCompactOperatorDescriptor compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                RuntimeComponentsProvider.RUNTIME_PROVIDER, RuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, typeTraits, comparatorFactories, blooFilterKeyFields, dataflowHelperFactory,
                NoOpOperationCallbackFactory.INSTANCE,
                metadataProvider.getStorageComponentProvider().getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                splitsAndConstraint.second);

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                splitsAndConstraint.second);
        spec.addRoot(compactOp);
        return spec;
    }
}
