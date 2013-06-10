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

import edu.uci.ics.asterix.metadata.feeds.IManagedFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An adapter that provides the functionality of receiving tweets from the
 * Twitter service in the form of ADM formatted records.
 */
public class PullBasedTwitterAdapter extends PullBasedAdapter implements IManagedFeedAdapter {

    private static final long serialVersionUID = 1L;

    public static final String QUERY = "query";
    public static final String INTERVAL = "interval";

    private ARecordType recordType;

    private PullBasedTwitterFeedClient tweetClient;

    @Override
    public IPullBasedFeedClient getFeedClient(int partition) {
        return tweetClient;
    }

    @Override
    public void configure(Map<String, Object> arguments) throws Exception {
        configuration = arguments;
        String[] fieldNames = { "id", "username", "location", "text", "timestamp" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.ASTRING };
        recordType = new ARecordType("FeedRecordType", fieldNames, fieldTypes, false);
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
        tweetClient = new PullBasedTwitterFeedClient(ctx, this);
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.READ;
    }

    @Override
    public void stop() {
        tweetClient.stop();
    }

    @Override
    public void alter(Map<String, Object> properties) {
        alterRequested = true;
    }

    public boolean isAlterRequested() {
        return alterRequested;
    }

    public void postAlteration() {
        alterRequested = false;
    }

    public ARecordType getAdapterOutputType() {
        return recordType;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        if (partitionConstraint == null) {
            partitionConstraint = new AlgebricksCountPartitionConstraint(1);
        }
        return partitionConstraint;
    }

}
