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

package org.apache.asterix.file;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.common.api.ILocalResourceMetadata;
import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import org.apache.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import org.apache.asterix.translator.CompiledStatements.CompiledDatasetDropStatement;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import org.apache.hyracks.storage.common.file.LocalResource;

public class DatasetOperations {

    private static Logger LOGGER = Logger.getLogger(DatasetOperations.class.getName());

    public static JobSpecification createDropDatasetJobSpec(CompiledDatasetDropStatement datasetDropStmt,
            AqlMetadataProvider metadataProvider)
                    throws AlgebricksException, HyracksDataException, RemoteException, ACIDException, AsterixException {

        String dataverseName = null;
        if (datasetDropStmt.getDataverseName() != null) {
            dataverseName = datasetDropStmt.getDataverseName();
        } else if (metadataProvider.getDefaultDataverse() != null) {
            dataverseName = metadataProvider.getDefaultDataverse().getDataverseName();
        }

        String datasetName = datasetDropStmt.getDatasetName();
        String datasetPath = dataverseName + File.separator + datasetName;

        LOGGER.info("DROP DATASETPATH: " + datasetPath);

        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("DROP DATASET: No metadata for dataset " + datasetName);
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return JobSpecificationUtils.createJobSpecification();
        }
        boolean temp = dataset.getDatasetDetails().isTemp();

        Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                dataverseName);
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }

        ARecordType itemType = (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(),
                dataset.getItemTypeName());

        ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, itemType);
        IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.computeFilterBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        int[] filterFields = DatasetUtils.createFilterFields(dataset);
        int[] btreeFields = DatasetUtils.createBTreeFieldsWhenThereisAFilter(dataset);
        JobSpecification specPrimary = JobSpecificationUtils.createJobSpecification();

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForDataset(dataset.getDataverseName(), datasetName, datasetName,
                        temp);
        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadataProvider.getMetadataTxnContext());

        IndexDropOperatorDescriptor primaryBtreeDrop = new IndexDropOperatorDescriptor(specPrimary,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first,
                new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                        compactionInfo.first, compactionInfo.second,
                        new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), true, filterTypeTraits, filterCmpFactories,
                        btreeFields, filterFields, !temp));
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specPrimary, primaryBtreeDrop,
                splitsAndConstraint.second);

        specPrimary.addRoot(primaryBtreeDrop);

        return specPrimary;
    }

    public static JobSpecification createDatasetJobSpec(Dataverse dataverse, String datasetName,
            AqlMetadataProvider metadata) throws AsterixException, AlgebricksException {
        String dataverseName = dataverse.getDataverseName();
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        Dataset dataset = metadata.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        ARecordType itemType = (ARecordType) metadata.findType(dataset.getItemTypeDataverseName(),
                dataset.getItemTypeName());
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
        int[] bloomFilterKeyFields = DatasetUtils.createBloomFilterKeyFields(dataset);

        ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, itemType);
        IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.computeFilterBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        int[] filterFields = DatasetUtils.createFilterFields(dataset);
        int[] btreeFields = DatasetUtils.createBTreeFieldsWhenThereisAFilter(dataset);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForDataset(dataverseName, datasetName, datasetName, temp);
        FileSplit[] fs = splitsAndConstraint.first.getFileSplits();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fs.length; i++) {
            sb.append(stringOf(fs[i]) + " ");
        }
        LOGGER.info("CREATING File Splits: " + sb.toString());

        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadata.getMetadataTxnContext());
        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
        //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
        ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(typeTraits,
                comparatorFactories, bloomFilterKeyFields, true, dataset.getDatasetId(), compactionInfo.first,
                compactionInfo.second, filterTypeTraits, filterCmpFactories, btreeFields, filterFields);
        ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                localResourceMetadata, LocalResource.LSMBTreeResource);

        TreeIndexCreateOperatorDescriptor indexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields,
                new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                        compactionInfo.first, compactionInfo.second,
                        new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), true, filterTypeTraits, filterCmpFactories,
                        btreeFields, filterFields, !temp),
                localResourceFactoryProvider, NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp,
                splitsAndConstraint.second);
        spec.addRoot(indexCreateOp);
        return spec;
    }

    private static String stringOf(FileSplit fs) {
        return fs.getNodeName() + ":" + fs.getLocalFile().toString();
    }

    public static JobSpecification compactDatasetJobSpec(Dataverse dataverse, String datasetName,
            AqlMetadataProvider metadata) throws AsterixException, AlgebricksException {
        String dataverseName = dataverse.getDataverseName();
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        Dataset dataset = metadata.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        boolean temp = dataset.getDatasetDetails().isTemp();

        ARecordType itemType = (ARecordType) metadata.findType(dataset.getItemTypeDataverseName(),
                dataset.getItemTypeName());
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
        int[] blooFilterKeyFields = DatasetUtils.createBloomFilterKeyFields(dataset);

        ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, itemType);
        IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.computeFilterBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        int[] filterFields = DatasetUtils.createFilterFields(dataset);
        int[] btreeFields = DatasetUtils.createBTreeFieldsWhenThereisAFilter(dataset);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForDataset(dataverseName, datasetName, datasetName, temp);

        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();

        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadata.getMetadataTxnContext());
        LSMTreeIndexCompactOperatorDescriptor compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, typeTraits, comparatorFactories, blooFilterKeyFields,
                new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                        compactionInfo.first, compactionInfo.second,
                        new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), true, filterTypeTraits, filterCmpFactories,
                        btreeFields, filterFields, !temp),
                NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                splitsAndConstraint.second);

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                splitsAndConstraint.second);
        spec.addRoot(compactOp);
        return spec;
    }
}
