package edu.uci.ics.asterix.file;

import edu.uci.ics.asterix.aql.translator.DdlTranslator.CompiledIndexDropStatement;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.context.AsterixRuntimeComponentsProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
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
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {
        SecondaryIndexCreator secondaryIndexCreator = SecondaryIndexCreator.createIndexCreator(createIndexStmt,
                metadata, physicalOptimizationConfig);
        return secondaryIndexCreator.buildCreationJobSpec();
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {
        SecondaryIndexCreator secondaryIndexCreator = SecondaryIndexCreator.createIndexCreator(createIndexStmt,
                metadata, physicalOptimizationConfig);
        return secondaryIndexCreator.buildLoadingJobSpec();
    }

    public static JobSpecification buildDropSecondaryIndexJobSpec(CompiledIndexDropStatement deleteStmt,
            AqlCompiledMetadataDeclarations datasetDecls) throws AlgebricksException, MetadataException {
        String datasetName = deleteStmt.getDatasetName();
        String indexName = deleteStmt.getIndexName();

        JobSpecification spec = new JobSpecification();

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = datasetDecls
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        IndexDropOperatorDescriptor btreeDrop = new IndexDropOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER, AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER,
                splitsAndConstraint.first, new LSMBTreeDataflowHelperFactory(
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER,
                        AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER));
        AlgebricksPartitionConstraintHelper
                .setPartitionConstraintInJobSpec(spec, btreeDrop, splitsAndConstraint.second);
        spec.addRoot(btreeDrop);

        return spec;
    }
}
