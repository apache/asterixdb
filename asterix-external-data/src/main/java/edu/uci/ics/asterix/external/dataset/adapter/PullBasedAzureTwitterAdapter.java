package edu.uci.ics.asterix.external.dataset.adapter;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Map;
import java.util.logging.Logger;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PullBasedAzureTwitterAdapter extends PullBasedAdapter implements IDatasourceAdapter {
    private static final Logger LOGGER = Logger.getLogger(PullBasedAzureTwitterAdapter.class.getName());

    private static final long serialVersionUID = 1L;

    private static final String ACCOUNT_NAME_KEY = "account-name";
    private static final String ACCOUNT_KEY_KEY = "account-key";
    private static final String TABLE_NAME_KEY = "table-name";
    private static final String PARTITIONS_KEY = "partitions";

    private final CloudStorageAccount csa;
    private final String connectionString;
    private final String azureAccountName;
    private final String azureAccountKey;
    private final ARecordType outputType;
    private final String tableName;
    private final boolean partitioned;

    private String[] lowKeys;
    private String[] highKeys;

    public PullBasedAzureTwitterAdapter(Map<String, String> configuration, IHyracksTaskContext ctx,
            ARecordType outputType) throws AsterixException {
        super(configuration, ctx);
        this.outputType = outputType;
        this.tableName = configuration.get(TABLE_NAME_KEY);
        if (tableName == null) {
            throw new IllegalArgumentException("You must specify a valid table name");
        }
        String partitionsString = configuration.get(PARTITIONS_KEY);
        if (partitionsString != null) {
            partitioned = true;
            configurePartitions(partitionsString);
        } else {
            partitioned = false;
        }
        azureAccountName = configuration.get(ACCOUNT_NAME_KEY);
        azureAccountKey = configuration.get(ACCOUNT_KEY_KEY);
        if (azureAccountName == null || azureAccountKey == null) {
            throw new IllegalArgumentException("You must specify a valid Azure account name and key");
        }
        connectionString = "DefaultEndpointsProtocol=http;" + "AccountName=" + azureAccountName + ";AccountKey="
                + azureAccountKey + ";";
        try {
            csa = CloudStorageAccount.parse(connectionString);
        } catch (InvalidKeyException | URISyntaxException e) {
            throw new IllegalArgumentException("You must specify a valid Azure account name and key", e);
        }
    }

    private void configurePartitions(String partitionsString) {
        String[] partitions = partitionsString.split(",");
        lowKeys = new String[partitions.length];
        highKeys = new String[partitions.length];
        for (int i = 0; i < partitions.length; ++i) {
            String[] loHi = partitions[i].split(":");
            lowKeys[i] = loHi[0];
            highKeys[i] = loHi[1];
        }
    }

    @Override
    public IPullBasedFeedClient getFeedClient(int partition) throws Exception {
        if (partitioned) {
            return new PullBasedAzureFeedClient(csa, outputType, tableName, lowKeys[partition], highKeys[partition]);
        }
        return new PullBasedAzureFeedClient(csa, outputType, tableName, null, null);
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PULL;
    }
}
