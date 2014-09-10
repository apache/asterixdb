/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.file;

import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;
import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.IAsterixPropertiesProvider;
import edu.uci.ics.asterix.common.context.AsterixVirtualBufferCacheProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import edu.uci.ics.asterix.metadata.feeds.ExternalDataScanOperatorDescriptor;
import edu.uci.ics.asterix.metadata.utils.ExternalDatasetsRegistry;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.resource.ExternalBTreeWithBuddyLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class SecondaryBTreeOperationsHelper extends SecondaryIndexOperationsHelper {

    protected SecondaryBTreeOperationsHelper(PhysicalOptimizationConfig physOptConf,
            IAsterixPropertiesProvider propertiesProvider) {
        super(physOptConf, propertiesProvider);
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
        ILocalResourceFactoryProvider localResourceFactoryProvider;
        IIndexDataflowHelperFactory indexDataflowHelperFactory;
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
            ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(secondaryTypeTraits,
                    secondaryComparatorFactories, secondaryBloomFilterKeyFields, true, dataset.getDatasetId(),
                    mergePolicyFactory, mergePolicyFactoryProperties, filterTypeTraits, filterCmpFactories,
                    secondaryBTreeFields, secondaryFilterFields);
            localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(localResourceMetadata,
                    LocalResource.LSMBTreeResource);
            indexDataflowHelperFactory = new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                    dataset.getDatasetId()), mergePolicyFactory, mergePolicyFactoryProperties,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), false, filterTypeTraits, filterCmpFactories,
                    secondaryBTreeFields, secondaryFilterFields);
        } else {
            // External dataset local resource and dataflow helper
            int[] buddyBreeFields = new int[] { numSecondaryKeys };
            ILocalResourceMetadata localResourceMetadata = new ExternalBTreeWithBuddyLocalResourceMetadata(
                    dataset.getDatasetId(), secondaryComparatorFactories, secondaryTypeTraits, mergePolicyFactory,
                    mergePolicyFactoryProperties, buddyBreeFields);
            localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(localResourceMetadata,
                    LocalResource.ExternalBTreeWithBuddyResource);
            indexDataflowHelperFactory = new ExternalBTreeWithBuddyDataflowHelperFactory(mergePolicyFactory,
                    mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), buddyBreeFields,
                    ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset));
        }
        TreeIndexCreateOperatorDescriptor secondaryIndexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                secondaryFileSplitProvider, secondaryTypeTraits, secondaryComparatorFactories,
                secondaryBloomFilterKeyFields, indexDataflowHelperFactory, localResourceFactoryProvider,
                NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryIndexCreateOp,
                secondaryPartitionConstraint);
        spec.addRoot(secondaryIndexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            /*
             * In case of external data, this method is used to build loading jobs for both initial load on index creation
             * and transaction load on dataset referesh
             */

            RecordDescriptor[] rDescs = new RecordDescriptor[] { new RecordDescriptor(new ISerializerDeserializer[] {}) };
            AlgebricksMetaOperatorDescriptor etsOp = new AlgebricksMetaOperatorDescriptor(spec, 0, 1,
                    new IPushRuntimeFactory[] { new EmptyTupleSourceRuntimeFactory() }, rDescs);
            // Create external indexing scan operator
            ExternalDataScanOperatorDescriptor primaryScanOp = createExternalIndexingOp(spec);

            // Assign op.
            AlgebricksMetaOperatorDescriptor asterixAssignOp = createExternalAssignOp(spec, numSecondaryKeys);

            // If any of the secondary fields are nullable, then add a select op that filters nulls.
            AlgebricksMetaOperatorDescriptor selectOp = null;
            if (anySecondaryKeyIsNullable) {
                selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys);
            }

            // Sort by secondary keys.
            ExternalSortOperatorDescriptor sortOp = createSortOp(spec, secondaryComparatorFactories, secondaryRecDesc);

            AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
            // Create secondary BTree bulk load op.
            AbstractTreeIndexOperatorDescriptor secondaryBulkLoadOp;
            ExternalBTreeWithBuddyDataflowHelperFactory dataflowHelperFactory = new ExternalBTreeWithBuddyDataflowHelperFactory(
                    mergePolicyFactory, mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(
                            dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), new int[] { numSecondaryKeys },
                    ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset));
            IOperatorDescriptor root;
            if (externalFiles != null) {
                // Transaction load
                secondaryBulkLoadOp = createExternalIndexBulkModifyOp(spec, numSecondaryKeys, dataflowHelperFactory,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR);
                root = secondaryBulkLoadOp;
            } else {
                // Initial load
                secondaryBulkLoadOp = createTreeIndexBulkLoadOp(spec, numSecondaryKeys, dataflowHelperFactory,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR);
                AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                        new IPushRuntimeFactory[] { new SinkRuntimeFactory() },
                        new RecordDescriptor[] { secondaryRecDesc });
                spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoadOp, 0, metaOp, 0);
                root = metaOp;
            }
            AlgebricksPartitionConstraintHelper
                    .setPartitionConstraintInJobSpec(spec, etsOp, primaryPartitionConstraint);
            // Connect the operators.
            spec.connect(new OneToOneConnectorDescriptor(spec), etsOp, 0, primaryScanOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, asterixAssignOp, 0);
            if (anySecondaryKeyIsNullable) {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, sortOp, 0);
            } else {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
            }
            spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
            spec.addRoot(root);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
            return spec;
        } else {
            // Create dummy key provider for feeding the primary index scan. 
            AbstractOperatorDescriptor keyProviderOp = createDummyKeyProviderOp(spec);

            // Create primary index scan op.
            BTreeSearchOperatorDescriptor primaryScanOp = createPrimaryIndexScanOp(spec);

            // Assign op.
            AlgebricksMetaOperatorDescriptor asterixAssignOp = createAssignOp(spec, primaryScanOp, numSecondaryKeys);

            // If any of the secondary fields are nullable, then add a select op that filters nulls.
            AlgebricksMetaOperatorDescriptor selectOp = null;
            if (anySecondaryKeyIsNullable) {
                selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys);
            }

            // Sort by secondary keys.
            ExternalSortOperatorDescriptor sortOp = createSortOp(spec, secondaryComparatorFactories, secondaryRecDesc);

            AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
            // Create secondary BTree bulk load op.
            TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = createTreeIndexBulkLoadOp(
                    spec,
                    numSecondaryKeys,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                            mergePolicyFactory, mergePolicyFactoryProperties,
                            new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate(), false, filterTypeTraits, filterCmpFactories,
                            secondaryBTreeFields, secondaryFilterFields), GlobalConfig.DEFAULT_TREE_FILL_FACTOR);

            AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                    new IPushRuntimeFactory[] { new SinkRuntimeFactory() }, new RecordDescriptor[] { secondaryRecDesc });
            // Connect the operators.
            spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, asterixAssignOp, 0);
            if (anySecondaryKeyIsNullable) {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, sortOp, 0);
            } else {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
            }
            spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoadOp, 0, metaOp, 0);
            spec.addRoot(metaOp);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
            return spec;
        }
    }

    protected int getNumSecondaryKeys() {
        return numSecondaryKeys;
    }

    @Override
    public JobSpecification buildCompactJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
        LSMTreeIndexCompactOperatorDescriptor compactOp;
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider, secondaryTypeTraits,
                    secondaryComparatorFactories, secondaryBloomFilterKeyFields, new LSMBTreeDataflowHelperFactory(
                            new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), mergePolicyFactory,
                            mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE,
                            storageProperties.getBloomFilterFalsePositiveRate(), false, filterTypeTraits,
                            filterCmpFactories, secondaryBTreeFields, secondaryFilterFields),
                    NoOpOperationCallbackFactory.INSTANCE);
        } else {
            // External dataset
            compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider, secondaryTypeTraits,
                    secondaryComparatorFactories, secondaryBloomFilterKeyFields,
                    new ExternalBTreeWithBuddyDataflowHelperFactory(mergePolicyFactory, mergePolicyFactoryProperties,
                            new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate(), new int[] { numSecondaryKeys },
                            ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset)),
                    NoOpOperationCallbackFactory.INSTANCE);
        }
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                secondaryPartitionConstraint);
        spec.addRoot(compactOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }
}
