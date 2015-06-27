package edu.uci.ics.asterix.external.dataset.adapter;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PullBasedAzureTwitterAdapter extends PullBasedAdapter implements IDatasourceAdapter {
    private static final Logger LOGGER = Logger.getLogger(PullBasedAzureTwitterAdapter.class.getName());

    private static final long serialVersionUID = 1L;

    private final CloudStorageAccount csa;
    private final String connectionString;
    private final String azureAccountName;
    private final String azureAccountKey;
    private final ARecordType outputType;
    private final String tableName;
    private final boolean partitioned;

    private String[] lowKeys;
    private String[] highKeys;

    public PullBasedAzureTwitterAdapter(String accountName, String accountKey, String tableName, String[] partitions,
            Map<String, String> configuration, IHyracksTaskContext ctx, ARecordType outputType) throws AsterixException {
        super(configuration, ctx);
        this.outputType = outputType;
        if (partitions != null) {
            partitioned = true;
            configurePartitions(partitions);
        } else {
            partitioned = false;
        }
        this.azureAccountName = accountName;
        this.azureAccountKey = accountKey;
        this.tableName = tableName;

        connectionString = "DefaultEndpointsProtocol=http;" + "AccountName=" + azureAccountName + ";AccountKey="
                + azureAccountKey + ";";
        try {
            csa = CloudStorageAccount.parse(connectionString);
        } catch (InvalidKeyException | URISyntaxException e) {
            throw new AsterixException("You must specify a valid Azure account name and key", e);
        }
    }

    private void configurePartitions(String[] partitions) {
        lowKeys = new String[partitions.length];
        highKeys = new String[partitions.length];
        for (int i = 0; i < partitions.length; ++i) {
            String[] loHi = partitions[i].split(":");
            lowKeys[i] = loHi[0];
            highKeys[i] = loHi[1];
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Partition " + i + " configured for keys " + lowKeys[i] + " to " + highKeys[i]);
            }
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

    @Override
    public boolean handleException(Exception e) {
        return false;
    }
}
