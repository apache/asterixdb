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
import edu.uci.ics.asterix.common.config.IAsterixPropertiesProvider;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.context.AsterixVirtualBufferCacheProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.data.operator.ExternalDataIndexingOperatorDescriptor;
import edu.uci.ics.asterix.external.util.ExternalIndexHashPartitionComputerFactory;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat;
import edu.uci.ics.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class SecondaryBTreeCreator extends SecondaryIndexCreator {

	protected SecondaryBTreeCreator(PhysicalOptimizationConfig physOptConf,
			IAsterixPropertiesProvider propertiesProvider) {
		super(physOptConf, propertiesProvider);
	}

	@Override
	public JobSpecification buildCreationJobSpec() throws AsterixException, AlgebricksException {
		JobSpecification spec = JobSpecificationUtils.createJobSpecification();
		AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
		//prepare a LocalResourceMetadata which will be stored in NC's local resource repository
		ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(
				secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories, secondaryBloomFilterKeyFields, true,
				dataset.getDatasetId());
		ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
				localResourceMetadata, LocalResource.LSMBTreeResource);

		TreeIndexCreateOperatorDescriptor secondaryIndexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
				AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER, AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER,
				secondaryFileSplitProvider, secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories,
				secondaryBloomFilterKeyFields, new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
						dataset.getDatasetId()), AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
						AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
						AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
						AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
						storageProperties.getBloomFilterFalsePositiveRate()), localResourceFactoryProvider,
						NoOpOperationCallbackFactory.INSTANCE);
		AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryIndexCreateOp,
				secondaryPartitionConstraint);
		spec.addRoot(secondaryIndexCreateOp);
		spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
		return spec;
	}

	@Override
	public JobSpecification buildLoadingJobSpec() throws AsterixException, AlgebricksException{
		if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
			JobSpecification spec = JobSpecificationUtils.createJobSpecification();
			Pair<ExternalDataIndexingOperatorDescriptor, AlgebricksPartitionConstraint> RIDScanOpAndConstraints;
			AlgebricksMetaOperatorDescriptor asterixAssignOp;
			try
			{
				//create external indexing scan operator
				RIDScanOpAndConstraints = createExternalIndexingOp(spec);

				//create assign operator
				asterixAssignOp = createExternalAssignOp(spec);
				AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
						RIDScanOpAndConstraints.second);
			}
			catch(Exception e)
			{
				throw new AsterixException("Failed to create external index scanning and loading job");
			}

			// If any of the secondary fields are nullable, then add a select op that filters nulls.
			AlgebricksMetaOperatorDescriptor selectOp = null;
			if (anySecondaryKeyIsNullable) {
				selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys,RIDScanOpAndConstraints.second);
			}

			// Sort by secondary keys.
			ExternalSortOperatorDescriptor sortOp = createSortOp(spec, secondaryComparatorFactories, secondaryRecDesc,RIDScanOpAndConstraints.second);
			// Create secondary BTree bulk load op.
			AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
			TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = createTreeIndexBulkLoadOp(
					spec,
					numSecondaryKeys,
					new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
							AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
							AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
							AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
							AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER, storageProperties
							.getBloomFilterFalsePositiveRate()), BTree.DEFAULT_FILL_FACTOR);
			IBinaryHashFunctionFactory[] hashFactories = DatasetUtils.computeExternalDataKeysBinaryHashFunFactories(dataset, NonTaggedDataFormat.INSTANCE.getBinaryHashFunctionFactoryProvider());

			//select partitioning keys (always the first 2 after secondary keys)
			int[] keys = new int[2];
			keys[0] = numSecondaryKeys;
			keys[1] = numSecondaryKeys + 1;

			IConnectorDescriptor hashConn = new MToNPartitioningConnectorDescriptor(spec,
					new ExternalIndexHashPartitionComputerFactory(keys, hashFactories));

			spec.connect(new OneToOneConnectorDescriptor(spec), RIDScanOpAndConstraints.first, 0, asterixAssignOp, 0);
			if (anySecondaryKeyIsNullable) {
				spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
				spec.connect(hashConn, selectOp, 0, sortOp, 0);
			} else {
				spec.connect(hashConn, asterixAssignOp, 0, sortOp, 0);
			}
			spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
			spec.addRoot(secondaryBulkLoadOp);
			spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
			return spec;
		}
		else
		{
			JobSpecification spec = JobSpecificationUtils.createJobSpecification();

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
							AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
							AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
							AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER,
							AsterixRuntimeComponentsProvider.LSMBTREE_SECONDARY_PROVIDER, storageProperties
							.getBloomFilterFalsePositiveRate()), BTree.DEFAULT_FILL_FACTOR);

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
}