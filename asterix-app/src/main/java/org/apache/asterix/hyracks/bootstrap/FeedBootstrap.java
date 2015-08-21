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
package edu.uci.ics.asterix.hyracks.bootstrap;

import edu.uci.ics.asterix.feeds.CentralFeedManager;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

public class FeedBootstrap {

    public final static String FEEDS_METADATA_DV = "feeds_metadata";
    public final static String FAILED_TUPLE_DATASET = "failed_tuple";
    public final static String FAILED_TUPLE_DATASET_TYPE = "FailedTupleType";
    public final static String FAILED_TUPLE_DATASET_KEY = "id";

    public static void setUpInitialArtifacts() throws Exception {

        StringBuilder builder = new StringBuilder();
        try {
            builder.append("create dataverse " + FEEDS_METADATA_DV + ";" + "\n");
            builder.append("use dataverse " + FEEDS_METADATA_DV + ";" + "\n");

            builder.append("create type " + FAILED_TUPLE_DATASET_TYPE + " as open { ");

            String[] fieldNames = new String[] { "id", "dataverseName", "feedName", "targetDataset", "tuple",
                    "message", "timestamp" };
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING };

            for (int i = 0; i < fieldNames.length; i++) {
                if (i > 0) {
                    builder.append(",");
                }
                builder.append(fieldNames[i] + ":");
                builder.append(fieldTypes[i].getTypeName());
            }
            builder.append("}" + ";" + "\n");

            builder.append("create dataset " + FAILED_TUPLE_DATASET + " " + "(" + FAILED_TUPLE_DATASET_TYPE + ")" + " "
                    + "primary key " + FAILED_TUPLE_DATASET_KEY + " on  " + MetadataConstants.METADATA_NODEGROUP_NAME
                    + ";");

            CentralFeedManager.AQLExecutor.executeAQL(builder.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: " + builder.toString());
            throw e;
        }
    }

}
