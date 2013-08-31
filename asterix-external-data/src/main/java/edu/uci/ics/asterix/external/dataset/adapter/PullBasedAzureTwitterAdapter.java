package edu.uci.ics.asterix.external.dataset.adapter;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Map;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PullBasedAzureTwitterAdapter extends PullBasedAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    private static final String ACCOUNT_NAME_KEY = "account_name";
    private static final String ACCOUNT_KEY_KEY = "account_key";
    private static final String TABLE_NAME_KEY = "table_name";

    private final CloudStorageAccount csa;
    private final String connectionString;
    private final String azureAccountName;
    private final String azureAccountKey;

    private final PullBasedAzureFeedClient feedClient;

    public PullBasedAzureTwitterAdapter(Map<String, String> configuration, IHyracksTaskContext ctx,
            ARecordType outputType) throws AsterixException {
        super(configuration, ctx);
        String tableName = configuration.get(TABLE_NAME_KEY);
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
        feedClient = new PullBasedAzureFeedClient(csa, outputType, tableName);
    }

    @Override
    public IPullBasedFeedClient getFeedClient(int partition) throws Exception {
        return feedClient;
    }
}
