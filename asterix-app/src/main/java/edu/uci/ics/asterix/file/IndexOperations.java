package edu.uci.ics.asterix.file;


import edu.uci.ics.asterix.aql.translator.DdlTranslator.CompiledIndexDropStatement;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.context.AsterixIndexRegistryProvider;
import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
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
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDropOperatorDescriptor;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class IndexOperations {

    private static final PhysicalOptimizationConfig physicalOptimizationConfig = OptimizationConfUtil
            .getPhysicalOptimizationConfig();

    public static JobSpecification buildSecondaryIndexCreationJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {
        SecondaryIndexCreator secondaryIndexCreator = SecondaryIndexCreator.createIndexCreator(createIndexStmt, metadata, physicalOptimizationConfig);
        return secondaryIndexCreator.buildCreationJobSpec();
    }
    
    public static JobSpecification buildSecondaryIndexLoadingJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {
        SecondaryIndexCreator secondaryIndexCreator = SecondaryIndexCreator.createIndexCreator(createIndexStmt, metadata, physicalOptimizationConfig);
        return secondaryIndexCreator.buildLoadingJobSpec();
    }
    
    public static JobSpecification buildDropSecondaryIndexJobSpec(CompiledIndexDropStatement deleteStmt,
            AqlCompiledMetadataDeclarations datasetDecls) throws AlgebricksException, MetadataException {
        String datasetName = deleteStmt.getDatasetName();
        String indexName = deleteStmt.getIndexName();

        JobSpecification spec = new JobSpecification();
        IIndexRegistryProvider<IIndex> indexRegistryProvider = AsterixIndexRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = datasetDecls
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        TreeIndexDropOperatorDescriptor btreeDrop = new TreeIndexDropOperatorDescriptor(spec, storageManager,
                indexRegistryProvider, splitsAndConstraint.first);
        AlgebricksPartitionConstraintHelper
                .setPartitionConstraintInJobSpec(spec, btreeDrop, splitsAndConstraint.second);
        spec.addRoot(btreeDrop);

        return spec;
    }

    public static void main(String[] args) throws Exception {
        String host;
        String appName;
        String ddlFile;

        switch (args.length) {
            case 0: {
                host = "127.0.0.1";
                appName = "asterix";
                ddlFile = "/home/abehm/workspace/asterix/asterix-app/src/test/resources/demo0927/local/create-index.aql";
                System.out.println("No arguments specified, using defauls:");
                System.out.println("HYRACKS HOST: " + host);
                System.out.println("APPNAME:      " + appName);
                System.out.println("DDLFILE:      " + ddlFile);
            }
                break;

            case 3: {
                host = args[0];
                appName = args[1];
                ddlFile = args[2];
            }
                break;

            default: {
                System.out.println("USAGE:");
                System.out.println("ARG 1: Hyracks Host (IP or Hostname)");
                System.out.println("ARG 2: Application Name (e.g., asterix)");
                System.out.println("ARG 3: DDL File");
                host = null;
                appName = null;
                ddlFile = null;
                System.exit(0);
            }
                break;

        }

        // int port = HyracksIntegrationUtil.DEFAULT_HYRACKS_CC_PORT;

        // AsterixJavaClient q = compileQuery(ddlFile, true, false, true);

        // long start = System.currentTimeMillis();
        // q.execute(port);
        // long end = System.currentTimeMillis();
        // System.err.println(start + " " + end + " " + (end - start));
    }
}
