package edu.uci.ics.asterix.metadata.feeds;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConstants.StatisticsConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FeedFrameTupleDecorator {

    private AMutableString aString = new AMutableString("");
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableInt32 aInt32 = new AMutableInt32(0);
    private AtomicInteger tupleId;

    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);

    private final int partition;
    private final ArrayBackedValueStorage attrNameStorage;
    private final ArrayBackedValueStorage attrValueStorage;

    public FeedFrameTupleDecorator(int partition) {
        this.tupleId = new AtomicInteger(0);
        this.partition = partition;
        this.attrNameStorage = new ArrayBackedValueStorage();
        this.attrValueStorage = new ArrayBackedValueStorage();
    }

    public void addLongAttribute(String attrName, long attrValue, IARecordBuilder recordBuilder)
            throws HyracksDataException, AsterixException {
        attrNameStorage.reset();
        aString.setValue(attrName);
        stringSerde.serialize(aString, attrNameStorage.getDataOutput());

        attrValueStorage.reset();
        aInt64.setValue(attrValue);
        int64Serde.serialize(aInt64, attrValueStorage.getDataOutput());

        recordBuilder.addField(attrNameStorage, attrValueStorage);
    }

    public void addIntegerAttribute(String attrName, int attrValue, IARecordBuilder recordBuilder)
            throws HyracksDataException, AsterixException {
        attrNameStorage.reset();
        aString.setValue(attrName);
        stringSerde.serialize(aString, attrNameStorage.getDataOutput());

        attrValueStorage.reset();
        aInt32.setValue(attrValue);
        int32Serde.serialize(aInt32, attrValueStorage.getDataOutput());

        recordBuilder.addField(attrNameStorage, attrValueStorage);
    }

    public void addTupleId(IARecordBuilder recordBuilder) throws HyracksDataException, AsterixException {
        addIntegerAttribute(StatisticsConstants.INTAKE_TUPLEID, tupleId.incrementAndGet(), recordBuilder);
    }

    public void addIntakePartition(IARecordBuilder recordBuilder) throws HyracksDataException, AsterixException {
        addIntegerAttribute(StatisticsConstants.INTAKE_PARTITION, partition, recordBuilder);
    }

    public void addIntakeTimestamp(IARecordBuilder recordBuilder) throws HyracksDataException, AsterixException {
        addLongAttribute(StatisticsConstants.INTAKE_TIMESTAMP, System.currentTimeMillis(), recordBuilder);
    }

    public void addStoreTimestamp(IARecordBuilder recordBuilder) throws HyracksDataException, AsterixException {
        addLongAttribute(StatisticsConstants.STORE_TIMESTAMP, System.currentTimeMillis(), recordBuilder);
    }

}
