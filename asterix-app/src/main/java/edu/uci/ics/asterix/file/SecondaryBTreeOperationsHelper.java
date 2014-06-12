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
import edu.uci.ics.asterix.common.config.IAsterixPropertiesProvider;
import edu.uci.ics.asterix.common.context.AsterixVirtualBufferCacheProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import edu.uci.ics.asterix.metadata.feeds.ExternalDataScanOperatorDescriptor;
import edu.uci.ics.asterix.metadata.utils.ExternalDatasetsRegistry;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.ExternalBTreeWithBuddyLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
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
            ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(
                    secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories, secondaryBloomFilterKeyFields,
                    true, dataset.getDatasetId(), mergePolicyFactory, mergePolicyFactoryProperties);
            localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(localResourceMetadata,
                    LocalResource.LSMBTreeResource);
            indexDataflowHelperFactory = new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                    dataset.getDatasetId()), mergePolicyFactory, mergePolicyFactoryProperties,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), false);
        } else {
            // External dataset local resource and dataflow helper
            int[] buddyBreeFields = new int[] { numSecondaryKeys };
            ILocalResourceMetadata localResourceMetadata = new ExternalBTreeWithBuddyLocalResourceMetadata(
                    dataset.getDatasetId(), secondaryComparatorFactories, secondaryRecDesc.getTypeTraits(),
                    mergePolicyFactory, mergePolicyFactoryProperties, buddyBreeFields);
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
                secondaryFileSplitProvider, secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories,
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
            if (externalFiles != null) {
                // Transaction load
                secondaryBulkLoadOp = createExternalIndexBulkModifyOp(spec, numSecondaryKeys, dataflowHelperFactory,
                        BTree.DEFAULT_FILL_FACTOR);
            } else {
                // Initial load
                secondaryBulkLoadOp = createTreeIndexBulkLoadOp(spec, numSecondaryKeys, dataflowHelperFactory,
                        BTree.DEFAULT_FILL_FACTOR);
            }
            // Connect the operators.
            spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, asterixAssignOp, 0);
            if (anySecondaryKeyIsNullable) {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, sortOp, 0);
            } else {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
            }
            spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
            spec.addRoot(secondaryBulkLoadOp);
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
                                    .getBloomFilterFalsePositiveRate(), false), BTree.DEFAULT_FILL_FACTOR);

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
            spec.addRoot(secondaryBulkLoadOp);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
            return spec;
        }
    }

    @Override
    public JobSpecification buildCompactJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
        LSMTreeIndexCompactOperatorDescriptor compactOp;
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider,
                    secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories, secondaryBloomFilterKeyFields,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                            mergePolicyFactory, mergePolicyFactoryProperties,
                            new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate(), false), NoOpOperationCallbackFactory.INSTANCE);
        } else {
            // External dataset
            compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider,
                    secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories, secondaryBloomFilterKeyFields,
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
