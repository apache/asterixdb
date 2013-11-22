/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An adapter that provides the functionality of receiving tweets from the
 * Twitter service in the form of ADM formatted records.
 */
public class PullBasedTwitterAdapter extends PullBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    public static final String QUERY = "query";
    public static final String INTERVAL = "interval";

    private ARecordType recordType;
    private PullBasedTwitterFeedClient tweetClient;

    @Override
    public IPullBasedFeedClient getFeedClient(int partition) {
        return tweetClient;
    }

    public PullBasedTwitterAdapter(Map<String, String> configuration, ARecordType recordType, IHyracksTaskContext ctx) throws AsterixException {
        super(configuration, ctx);
        tweetClient = new PullBasedTwitterFeedClient(ctx, recordType, this);
    }

    public ARecordType getAdapterOutputType() {
        return recordType;
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PULL;
    }

}
