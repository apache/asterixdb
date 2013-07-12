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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
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
        List<String> candidateIngestionNodes = new ArrayList<String>();
        List<String> storageNodes = new ArrayList<String>();
        Set<String> allNodes = AsterixAppContextInfo.getInstance().getMetadataProperties().getNodeNames();
        candidateIngestionNodes.addAll(allNodes);
        String dvds = configuration.get(KEY_DATAVERSE_DATASET);
        if (dvds != null) {
            String[] components = dvds.split(":");
            String dataverse = components[0];
            String dataset = components[1];
            MetadataTransactionContext ctx = null;
            NodeGroup ng = null;
            try {
                MetadataManager.INSTANCE.acquireReadLatch();
                ctx = MetadataManager.INSTANCE.beginTransaction();
                Dataset ds = MetadataManager.INSTANCE.getDataset(ctx, dataverse, dataset);
                String nodegroupName = ((FeedDatasetDetails) ds.getDatasetDetails()).getNodeGroupName();
                ng = MetadataManager.INSTANCE.getNodegroup(ctx, nodegroupName);
                MetadataManager.INSTANCE.commitTransaction(ctx);
            } catch (Exception e) {
                if (ctx != null) {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                }
                throw e;
            } finally {
                MetadataManager.INSTANCE.releaseReadLatch();
            }
            storageNodes = ng.getNodeNames();
            candidateIngestionNodes.removeAll(storageNodes);
        }

        String iCardinalityParam = (String) configuration.get(KEY_INGESTION_CARDINALITY);
        int requiredCardinality = iCardinalityParam != null ? Integer.parseInt(iCardinalityParam) : 1;
        String[] ingestionLocations = new String[requiredCardinality];
        String[] candidateNodesArray = candidateIngestionNodes.toArray(new String[] {});
        if (requiredCardinality > candidateIngestionNodes.size()) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning(" Ingestion nodes overlap with storage nodes");
            }
            int numChosen = 0;
            for (int i = 0; i < candidateNodesArray.length; i++, numChosen++) {
                ingestionLocations[i] = candidateNodesArray[i];
            }

            for (int j = numChosen, k = 0; j < requiredCardinality && k < storageNodes.size(); j++, k++, numChosen++) {
                ingestionLocations[j] = storageNodes.get(k);
            }

            if (numChosen < requiredCardinality) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Multiple ingestion tasks per node.");
                }
                for (int j = numChosen, k = 0; j < requiredCardinality; j++, k++) {
                    ingestionLocations[j] = candidateNodesArray[k];
                }
            }
        } else {
            Random r = new Random();
            int ingestLocIndex = r.nextInt(candidateIngestionNodes.size());
            ingestionLocations[0] = candidateNodesArray[ingestLocIndex];
            for (int i = 1; i < requiredCardinality; i++) {
                ingestionLocations[i] = candidateNodesArray[(ingestLocIndex + i) % candidateNodesArray.length];
            }
        }
        return new AlgebricksAbsolutePartitionConstraint(ingestionLocations);
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