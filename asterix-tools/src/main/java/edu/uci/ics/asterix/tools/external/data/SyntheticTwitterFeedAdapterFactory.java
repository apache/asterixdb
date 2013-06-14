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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory.AdapterType;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory.SupportedOperation;
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
 * Factory class for creating @see{RateControllerFileSystemBasedAdapter} The
 * adapter simulates a feed from the contents of a source file. The file can be
 * on the local file system or on HDFS. The feed ends when the content of the
 * source file has been ingested.
 */
public class SyntheticTwitterFeedAdapterFactory implements ITypedAdapterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private Map<String, Object> configuration;

    private static final String KEY_DATAVERSE_DATASET = "dataverse-dataset";

    private static final ARecordType outputType = initOutputType();

    @Override
    public String getName() {
        return "synthetic_twitter_feed";
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
    public void configure(Map<String, Object> configuration) throws Exception {
        this.configuration = configuration;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        String dvds = (String) configuration.get(KEY_DATAVERSE_DATASET);
        String[] components = dvds.split(":");
        String dataverse = components[0];
        String dataset = components[1];
        MetadataTransactionContext ctx = null;
        NodeGroup ng = null;
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            Dataset ds = MetadataManager.INSTANCE.getDataset(ctx, dataverse, dataset);
            String nodegroupName = ((FeedDatasetDetails) ds.getDatasetDetails()).getNodeGroupName();
            ng = MetadataManager.INSTANCE.getNodegroup(ctx, nodegroupName);
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(ctx);
            throw e;
        }
        List<String> storageNodes = ng.getNodeNames();
        Set<String> nodes = AsterixAppContextInfo.getInstance().getMetadataProperties().getNodeNames();
        nodes.removeAll(storageNodes);
        Random r = new Random();
        String ingestionLocation = nodes.toArray(new String[] {})[r.nextInt(nodes.size())];
        return new AlgebricksAbsolutePartitionConstraint(new String[] { ingestionLocation });
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx) throws Exception {
        return new SyntheticTwitterFeedAdapter(configuration, outputType, ctx);
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