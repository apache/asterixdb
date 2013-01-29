package edu.uci.ics.asterix.file;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.context.AsterixRuntimeComponentsProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledCreateIndexStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledIndexDropStatement;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;

public class IndexOperations {

    private static final PhysicalOptimizationConfig physicalOptimizationConfig = OptimizationConfUtil
            .getPhysicalOptimizationConfig();

    public static JobSpecification buildSecondaryIndexCreationJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlMetadataProvider metadataProvider) throws AsterixException, AlgebricksException {
        SecondaryIndexCreator secondaryIndexCreator = SecondaryIndexCreator.createIndexCreator(createIndexStmt,
                metadataProvider, physicalOptimizationConfig);
        return secondaryIndexCreator.buildCreationJobSpec();
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlMetadataProvider metadataProvider) throws AsterixException, AlgebricksException {
        SecondaryIndexCreator secondaryIndexCreator = SecondaryIndexCreator.createIndexCreator(createIndexStmt,
                metadataProvider, physicalOptimizationConfig);
        return secondaryIndexCreator.buildLoadingJobSpec();
    }

    public static JobSpecification buildDropSecondaryIndexJobSpec(CompiledIndexDropStatement indexDropStmt,
            AqlMetadataProvider metadataProvider) throws AlgebricksException, MetadataException {
        String dataverseName = indexDropStmt.getDataverseName() == null ? metadataProvider.getDefaultDataverseName()
                : indexDropStmt.getDataverseName();
        String datasetName = indexDropStmt.getDatasetName();
        String indexName = indexDropStmt.getIndexName();

        JobSpecification spec = new JobSpecification();

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(dataverseName, datasetName, indexName);
        IndexDropOperatorDescriptor btreeDrop = new IndexDropOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER, AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER,
                splitsAndConstraint.first, new LSMBTreeDataflowHelperFactory(
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER, GlobalConfig.DEFAULT_INDEX_MEM_PAGE_SIZE,
                        GlobalConfig.DEFAULT_INDEX_MEM_NUM_PAGES));
        AlgebricksPartitionConstraintHelper
                .setPartitionConstraintInJobSpec(spec, btreeDrop, splitsAndConstraint.second);
        spec.addRoot(btreeDrop);

        return spec;
    }
}
