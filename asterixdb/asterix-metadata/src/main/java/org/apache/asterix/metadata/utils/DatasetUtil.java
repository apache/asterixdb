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
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import org.apache.hyracks.storage.common.IResourceFactory;

public class DatasetUtil {
    private static final Logger LOGGER = Logger.getLogger(DatasetUtil.class.getName());
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

    public static int[] createFilterFields(Dataset dataset) throws AlgebricksException {
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

    public static int[] createBTreeFieldsWhenThereisAFilter(Dataset dataset) throws AlgebricksException {
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

    public static int getPositionOfPartitioningKeyField(Dataset dataset, String fieldExpr) {
        List<List<String>> partitioningKeys = dataset.getPrimaryKeys();
        for (int i = 0; i < partitioningKeys.size(); i++) {
            if ((partitioningKeys.get(i).size() == 1) && partitioningKeys.get(i).get(0).equals(fieldExpr)) {
                return i;
            }
        }
        return -1;
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

    public static JobSpecification dropDatasetJobSpec(Dataset dataset, MetadataProvider metadataProvider)
            throws AlgebricksException, HyracksDataException, RemoteException, ACIDException {
        String datasetPath = dataset.getDataverseName() + File.separator + dataset.getDatasetName();
        LOGGER.info("DROP DATASETPATH: " + datasetPath);
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

    public static JobSpecification createDatasetJobSpec(Dataverse dataverse, String datasetName,
            MetadataProvider metadataProvider) throws AlgebricksException {
        String dataverseName = dataverse.getDataverseName();
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        Index index = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                datasetName, datasetName);
        ARecordType itemType =
                (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        // get meta item type
        ARecordType metaItemType = null;
        if (dataset.hasMetaPart()) {
            metaItemType = (ARecordType) metadataProvider.findType(dataset.getMetaItemTypeDataverseName(),
                    dataset.getMetaItemTypeName());
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
        //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
        IResourceFactory resourceFactory = dataset.getResourceFactory(metadataProvider, index, itemType, metaItemType,
                compactionInfo.first, compactionInfo.second);
        IndexBuilderFactory indexBuilderFactory =
                new IndexBuilderFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
                        splitsAndConstraint.first, resourceFactory, !dataset.isTemp());
        IndexCreateOperatorDescriptor indexCreateOp = new IndexCreateOperatorDescriptor(spec, indexBuilderFactory);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp,
                splitsAndConstraint.second);
        spec.addRoot(indexCreateOp);
        return spec;
    }

    public static JobSpecification compactDatasetJobSpec(Dataverse dataverse, String datasetName,
            MetadataProvider metadataProvider) throws AsterixException, AlgebricksException {
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

    public static boolean isFullyQualifiedName(String datasetName) {
        return datasetName.indexOf('.') > 0; //NOSONAR a fully qualified name can't start with a .
    }

    public static String getDataverseFromFullyQualifiedName(String datasetName) {
        int idx = datasetName.indexOf('.');
        return datasetName.substring(0, idx);
    }
}
