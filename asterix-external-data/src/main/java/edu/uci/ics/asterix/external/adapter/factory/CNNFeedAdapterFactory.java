package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.CNNFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;

public class CNNFeedAdapterFactory implements ITypedFeedDatasetAdapterFactory {

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration) throws Exception {
        CNNFeedAdapter cnnFeedAdapter = new CNNFeedAdapter();
        cnnFeedAdapter.configure(configuration);
        return cnnFeedAdapter;
    }

    @Override
    public String getName() {
        return "cnn_feed";
    }

    @Override
    public FeedAdapterType getFeedAdapterType() {
        return FeedAdapterType.TYPED;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.FEED;
    }

}
