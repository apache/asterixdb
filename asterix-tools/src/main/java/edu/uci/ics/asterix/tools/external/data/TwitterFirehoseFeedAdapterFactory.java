/*
x * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.tools.external.data;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating @see{TwitterFirehoseFeedAdapter}.
 * The adapter simulates a twitter firehose with tweets being "pushed" into Asterix at a configurable rate
 * measured in terms of TPS (tweets/second). The stream of tweets lasts for a configurable duration (measured in seconds).
 */
public class TwitterFirehoseFeedAdapterFactory extends StreamBasedAdapterFactory implements ITypedAdapterFactory {

    private static final long serialVersionUID = 1L;

    /*
     * The dataverse and dataset names for the target feed dataset. This informaiton 
     * is used in configuring partition constraints for the adapter. It is preferred that 
     * the adapter location does not coincide with a partition location for the feed dataset.
     */
    private static final String KEY_DATAVERSE_DATASET = "dataverse-dataset";

    /*
     * Degree of parallelism for feed ingestion activity. Defaults to 1.
     */
    private static final String KEY_INGESTION_CARDINALITY = "ingestion-cardinality";

    private static final ARecordType outputType = initOutputType();

    @Override
    public String getName() {
        return "twitter_firehose";
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        configuration.put(KEY_FORMAT, FORMAT_ADM);
        this.configuration = configuration;
        this.configureFormat(initOutputType());
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        String ingestionCardinalityParam = (String) configuration.get(KEY_INGESTION_CARDINALITY);
        int requiredCardinality = ingestionCardinalityParam != null ? Integer.parseInt(ingestionCardinalityParam) : 1;
        return new AlgebricksCountPartitionConstraint(requiredCardinality);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx) throws Exception {
        return new TwitterFirehoseFeedAdapter(configuration, parserFactory, outputType, ctx);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    private static ARecordType initOutputType() {
        ARecordType outputType = null;
        try {
            String[] userFieldNames = new String[] { "screen-name", "lang", "friends_count", "statuses_count", "name",
                    "followers_count" };

            IAType[] userFieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32,
                    BuiltinType.AINT32, BuiltinType.ASTRING, BuiltinType.AINT32 };
            ARecordType userRecordType = new ARecordType("TwitterUserType", userFieldNames, userFieldTypes, false);

            String[] fieldNames = new String[] { "tweetid", "user", "sender-location", "send-time", "referred-topics",
                    "message-text" };

            AUnorderedListType unorderedListType = new AUnorderedListType(BuiltinType.ASTRING, "referred-topics");
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, userRecordType, BuiltinType.APOINT,
                    BuiltinType.ADATETIME, unorderedListType, BuiltinType.ASTRING };
            outputType = new ARecordType("TweetMessageType", fieldNames, fieldTypes, false);

        } catch (AsterixException e) {
            throw new IllegalStateException("Unable to initialize output type");
        }
        return outputType;
    }
}