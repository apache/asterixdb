package edu.uci.ics.asterix.feeds;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetadataManager;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedBootstrap;
import edu.uci.ics.asterix.metadata.feeds.XAQLFeedMessage;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FeedMetadataManager implements IFeedMetadataManager {

    private static final Logger LOGGER = Logger.getLogger(FeedMetadataManager.class.getName());

    private final String nodeId;
    private ARecordType recordType;

    public FeedMetadataManager(String nodeId) throws AsterixException, HyracksDataException {
        this.nodeId = nodeId;
        String[] fieldNames = new String[] { "id", "dataverseName", "feedName", "targetDataset", "tuple", "message",
                "timestamp" };
        IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING };

        recordType = new ARecordType(FeedBootstrap.FAILED_TUPLE_DATASET_TYPE, fieldNames, fieldTypes, true);
    }

    @Override
    public void logTuple(FeedConnectionId connectionId, String tuple, String message, IFeedManager feedManager)
            throws AsterixException {
        try {
            AString id = new AString("1");
            AString dataverseValue = new AString(connectionId.getFeedId().getDataverse());
            AString feedValue = new AString(connectionId.getFeedId().getFeedName());
            AString targetDatasetValue = new AString(connectionId.getDatasetName());
            AString tupleValue = new AString(tuple);
            AString messageValue = new AString(message);
            AString dateTime = new AString(new Date().toString());

            IAObject[] fields = new IAObject[] { id, dataverseValue, feedValue, targetDatasetValue, tupleValue,
                    messageValue, dateTime };
            ARecord record = new ARecord(recordType, fields);
            StringBuilder builder = new StringBuilder();
            builder.append("use dataverse " + FeedBootstrap.FEEDS_METADATA_DV + ";" + "\n");
            builder.append("insert into dataset " + FeedBootstrap.FAILED_TUPLE_DATASET + " ");
            builder.append(" (" + recordToString(record) + ")");
            builder.append(";");

            XAQLFeedMessage xAqlMessage = new XAQLFeedMessage(connectionId, builder.toString());
            feedManager.getFeedMessageService().sendMessage(xAqlMessage);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" Sent " + xAqlMessage.toJSON());
            }
        } catch (Exception pe) {
            throw new AsterixException(pe);
        }
    }

    @Override
    public String toString() {
        return "FeedMetadataManager [" + nodeId + "]";
    }

    private String recordToString(ARecord record) {
        String[] fieldNames = record.getType().getFieldNames();
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("\"" + fieldNames[i] + "\"");
            sb.append(": ");
            switch (record.getType().getFieldTypes()[i].getTypeTag()) {
                case STRING:
                    sb.append("\"" + ((AString) record.getValueByPos(i)).getStringValue() + "\"");
                    break;
            }
        }
        sb.append(" }");
        return sb.toString();
    }
}
