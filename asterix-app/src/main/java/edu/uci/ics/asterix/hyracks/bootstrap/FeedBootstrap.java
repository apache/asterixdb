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
