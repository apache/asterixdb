package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.dataset.adapter.PullBasedAzureTwitterAdapter;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PullBasedAzureTwitterAdapterFactory implements ITypedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private static final String INGESTOR_LOCATIONS_KEY = "ingestor-locations";
    private static final String PARTITIONS_KEY = "partitions";
    private static final String OUTPUT_TYPE_KEY = "output-type";
    private static final String TABLE_NAME_KEY = "table-name";
    private static final String ACCOUNT_NAME_KEY = "account-name";
    private static final String ACCOUNT_KEY_KEY = "account-key";

    private ARecordType recordType;
    private Map<String, String> configuration;
    private String tableName;
    private String azureAccountName;
    private String azureAccountKey;
    private String[] locations;
    private String[] partitions;

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public String getName() {
        return "azure_twitter";
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        String locationsStr = configuration.get(INGESTOR_LOCATIONS_KEY);
        if (locationsStr == null) {
            return null;
        }
        String[] locations = locationsStr.split(",");
        return new AlgebricksAbsolutePartitionConstraint(locations);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        return new PullBasedAzureTwitterAdapter(azureAccountName, azureAccountKey, tableName, partitions,
                configuration, ctx, recordType);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return recordType;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;

        tableName = configuration.get(TABLE_NAME_KEY);
        if (tableName == null) {
            throw new AsterixException("You must specify a valid table name");
        }
        azureAccountName = configuration.get(ACCOUNT_NAME_KEY);
        azureAccountKey = configuration.get(ACCOUNT_KEY_KEY);
        if (azureAccountName == null || azureAccountKey == null) {
            throw new AsterixException("You must specify a valid Azure account name and key");
        }

        int nIngestLocations = 1;
        String locationsStr = configuration.get(INGESTOR_LOCATIONS_KEY);
        if (locationsStr != null) {
            locations = locationsStr.split(",");
            nIngestLocations = locations.length;
        }

        int nPartitions = 1;
        String partitionsStr = configuration.get(PARTITIONS_KEY);
        if (partitionsStr != null) {
            partitions = partitionsStr.split(",");
            nPartitions = partitions.length;
        }

        if (nIngestLocations != nPartitions) {
            throw new AsterixException("Invalid adapter configuration: number of ingestion-locations ("
                    + nIngestLocations + ") must be the same as the number of partitions (" + nPartitions + ")");
        }
        configureType();
    }

    private void configureType() throws Exception {
        String fqOutputType = configuration.get(OUTPUT_TYPE_KEY);

        if (fqOutputType == null) {
            throw new IllegalArgumentException("No output type specified");
        }
        String[] dataverseAndType = fqOutputType.split("[.]");
        String dataverseName = dataverseAndType[0];
        String datatypeName = dataverseAndType[1];

        MetadataTransactionContext ctx = null;
        MetadataManager.INSTANCE.acquireReadLatch();
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            Datatype t = MetadataManager.INSTANCE.getDatatype(ctx, dataverseName, datatypeName);
            IAType type = t.getDatatype();
            if (type.getTypeTag() != ATypeTag.RECORD) {
                throw new IllegalStateException();
            }
            recordType = (ARecordType) t.getDatatype();
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (Exception e) {
            if (ctx != null) {
                MetadataManager.INSTANCE.abortTransaction(ctx);
            }
            throw e;
        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
    }
}
