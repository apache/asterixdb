package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public interface IFeedClientFactory {

    public IPullBasedFeedClient createFeedClient(IHyracksTaskContext ctx, Map<String, String> configuration)
            throws Exception;

    public ARecordType getRecordType() throws AsterixException;

    public FeedClientType getFeedClientType();

    public enum FeedClientType {
        GENERIC,
        TYPED
    }
}
