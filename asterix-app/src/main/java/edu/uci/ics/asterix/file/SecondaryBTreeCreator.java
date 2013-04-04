package edu.uci.ics.asterix.file;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.context.AsterixRuntimeComponentsProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.transaction.management.resource.ILocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
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
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class SecondaryBTreeCreator extends SecondaryIndexCreator {

    protected SecondaryBTreeCreator(PhysicalOptimizationConfig physOptConf) {
        super(physOptConf);
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = new JobSpecification();

        //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
        ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(
                secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories, secondaryBloomFilterKeyFields, false,
                GlobalConfig.DEFAULT_INDEX_MEM_PAGE_SIZE, GlobalConfig.DEFAULT_INDEX_MEM_NUM_PAGES);
        ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                localResourceMetadata, LocalResource.LSMBTreeResource);

        TreeIndexCreateOperatorDescriptor secondaryIndexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER, AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER,
                secondaryFileSplitProvider, secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories,
                secondaryBloomFilterKeyFields, new LSMBTreeDataflowHelperFactory(
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER, GlobalConfig.DEFAULT_INDEX_MEM_PAGE_SIZE,
                        GlobalConfig.DEFAULT_INDEX_MEM_NUM_PAGES), localResourceFactoryProvider,
                NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryIndexCreateOp,
                secondaryPartitionConstraint);
        spec.addRoot(secondaryIndexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = new JobSpecification();

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

        // Create secondary BTree bulk load op.
        TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = createTreeIndexBulkLoadOp(spec, numSecondaryKeys,
                new LSMBTreeDataflowHelperFactory(AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER, GlobalConfig.DEFAULT_INDEX_MEM_PAGE_SIZE,
                        GlobalConfig.DEFAULT_INDEX_MEM_NUM_PAGES), BTree.DEFAULT_FILL_FACTOR);

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
