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
package edu.uci.ics.asterix.tools.external.data;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class SocketClientAdapterFactory implements ITypedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private static final ARecordType outputType = initOutputType();

    private GenericSocketFeedAdapterFactory genericSocketAdapterFactory;

    private String[] fileSplits;

    public static final String KEY_FILE_SPLITS = "file_splits";

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
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
            IAType[] fieldTypes = new IAType[] { BuiltinType.AINT64, userRecordType, BuiltinType.APOINT,
                    BuiltinType.ADATETIME, unorderedListType, BuiltinType.ASTRING };
            outputType = new ARecordType("TweetMessageType", fieldNames, fieldTypes, false);

        } catch (AsterixException | HyracksDataException e) {
            throw new IllegalStateException("Unable to initialize output type");
        }
        return outputType;
    }

    @Override
    public String getName() {
        return "socket_client";
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return genericSocketAdapterFactory.getPartitionConstraint();
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        Pair<String, Integer> socket = genericSocketAdapterFactory.getSockets().get(partition);
        return new SocketClientAdapter(socket.second, fileSplits[partition], ctx);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        String fileSplitsValue = configuration.get(KEY_FILE_SPLITS);
        if (fileSplitsValue == null) {
            throw new IllegalArgumentException(
                    "File splits not specified. File split is specified as a comma separated list of paths");
        }
        fileSplits = fileSplitsValue.trim().split(",");
        genericSocketAdapterFactory = new GenericSocketFeedAdapterFactory();
        genericSocketAdapterFactory.configure(configuration, outputType);
    }
}
