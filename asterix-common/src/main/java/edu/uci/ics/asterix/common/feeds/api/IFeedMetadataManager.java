package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;

public interface IFeedMetadataManager {

    /**
     * @param feedConnectionId
     *            connection id corresponding to the feed connection
     * @param tuple
     *            the erroneous tuple that raised an exception
     * @param message
     *            the message corresponding to the exception being raised
     * @param feedManager
     * @throws AsterixException
     */
    public void logTuple(FeedConnectionId feedConnectionId, String tuple, String message, IFeedManager feedManager)
            throws AsterixException;

}
