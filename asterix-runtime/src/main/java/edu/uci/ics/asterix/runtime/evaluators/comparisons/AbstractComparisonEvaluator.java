package edu.uci.ics.asterix.runtime.evaluators.comparisons;

import java.io.DataOutput;

import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.ADateOrTimeAscBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.ADateTimeAscBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public abstract class AbstractComparisonEvaluator implements ICopyEvaluator {

    protected enum ComparisonResult {
        LESS_THAN,
        EQUAL,
        GREATER_THAN,
        UNKNOWN
    };

    protected DataOutput out;
    protected ArrayBackedValueStorage outLeft = new ArrayBackedValueStorage();
    protected ArrayBackedValueStorage outRight = new ArrayBackedValueStorage();
    protected ICopyEvaluator evalLeft;
    protected ICopyEvaluator evalRight;
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    protected IBinaryComparator strBinaryComp = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator dateTimeBinaryComp = ADateTimeAscBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator dateOrTimeBinaryComp = ADateOrTimeAscBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();

    public AbstractComparisonEvaluator(DataOutput out, ICopyEvaluatorFactory evalLeftFactory,
            ICopyEvaluatorFactory evalRightFactory) throws AlgebricksException {
        this.out = out;
        this.evalLeft = evalLeftFactory.createEvaluator(outLeft);
        this.evalRight = evalRightFactory.createEvaluator(outRight);
    }

    protected void evalInputs(IFrameTupleReference tuple) throws AlgebricksException {
        outLeft.reset();
        evalLeft.evaluate(tuple);
        outRight.reset();
        evalRight.evaluate(tuple);
    }

    protected ComparisonResult compareResults() throws AlgebricksException {
        boolean isLeftNull = false;
        boolean isRightNull = false;
        ATypeTag typeTag1 = null;
        ATypeTag typeTag2 = null;

        if (outLeft.getLength() == 0) {
            isLeftNull = true;
        } else {
            typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outLeft.getByteArray()[0]);
            if (typeTag1 == ATypeTag.NULL) {
                isLeftNull = true;
            }
        }
        if (outRight.getLength() == 0) {
            isRightNull = true;
        } else {
            typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outRight.getByteArray()[0]);
            if (typeTag2 == ATypeTag.NULL) {
                isRightNull = true;
            }
        }

        if (isLeftNull || isRightNull)
            return ComparisonResult.UNKNOWN;

        switch (typeTag1) {
            case INT8: {
                return compareInt8WithArg(typeTag2);
            }
            case INT16: {
                return compareInt16WithArg(typeTag2);
            }
            case INT32: {
                return compareInt32WithArg(typeTag2);
            }
            case INT64: {
                return compareInt64WithArg(typeTag2);
            }
            case FLOAT: {
                return compareFloatWithArg(typeTag2);
            }
            case DOUBLE: {
                return compareDoubleWithArg(typeTag2);
            }
            case STRING: {
                return compareStringWithArg(typeTag2);
            }
            case BOOLEAN: {
                return compareBooleanWithArg(typeTag2);
            }
            case DATETIME: {
                return compareDateTimeWithArg(typeTag2);
            }
            case DATE:
            case TIME: {
                return compareDateOrTimeWithArg(typeTag2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types " + typeTag1 + " and " + typeTag2
                        + " .");
            }
        }
    }

    private ComparisonResult compareDateOrTimeWithArg(ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag2 == ATypeTag.NULL) {
            return ComparisonResult.GREATER_THAN;
        } else if (typeTag2 == ATypeTag.DATETIME) {
            int result = dateOrTimeBinaryComp.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                    outRight.getByteArray(), 1, outRight.getLength() - 1);
            if (result == 0)
                return ComparisonResult.EQUAL;
            else if (result < 0)
                return ComparisonResult.LESS_THAN;
            else
                return ComparisonResult.GREATER_THAN;
        }
        throw new AlgebricksException("Comparison is undefined between types ADateTime and " + typeTag2 + " .");
    }

    private ComparisonResult compareDateTimeWithArg(ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag2 == ATypeTag.NULL) {
            return ComparisonResult.GREATER_THAN;
        } else if (typeTag2 == ATypeTag.DATETIME) {
            int result = dateTimeBinaryComp.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                    outRight.getByteArray(), 1, outRight.getLength() - 1);
            if (result == 0)
                return ComparisonResult.EQUAL;
            else if (result < 0)
                return ComparisonResult.LESS_THAN;
            else
                return ComparisonResult.GREATER_THAN;
        }
        throw new AlgebricksException("Comparison is undefined between types ADateTime and " + typeTag2 + " .");
    }

    private ComparisonResult compareBooleanWithArg(ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag2 == ATypeTag.BOOLEAN) {
            byte b0 = outLeft.getByteArray()[1];
            byte b1 = outRight.getByteArray()[1];
            return compareByte(b0, b1);
        }
        throw new AlgebricksException("Comparison is undefined between types ABoolean and " + typeTag2 + " .");
    }

    private ComparisonResult compareStringWithArg(ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag2 == ATypeTag.STRING) {
            int result = strBinaryComp.compare(outLeft.getByteArray(), 1, outLeft.getLength() - 1,
                    outRight.getByteArray(), 1, outRight.getLength() - 1);
            if (result == 0)
                return ComparisonResult.EQUAL;
            else if (result < 0)
                return ComparisonResult.LESS_THAN;
            else
                return ComparisonResult.GREATER_THAN;
        }
        throw new AlgebricksException("Comparison is undefined between types AString and " + typeTag2 + " .");
    }

    private ComparisonResult compareDoubleWithArg(ATypeTag typeTag2) throws AlgebricksException {
        double s = ADoubleSerializerDeserializer.getDouble(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types ADouble and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareFloatWithArg(ATypeTag typeTag2) throws AlgebricksException {
        float s = FloatSerializerDeserializer.getFloat(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AFloat and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt64WithArg(ATypeTag typeTag2) throws AlgebricksException {
        long s = AInt64SerializerDeserializer.getLong(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt64 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt32WithArg(ATypeTag typeTag2) throws AlgebricksException {
        int s = IntegerSerializerDeserializer.getInt(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt32 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt16WithArg(ATypeTag typeTag2) throws AlgebricksException {
        short s = AInt16SerializerDeserializer.getShort(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareShort(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareShort(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt16 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt8WithArg(ATypeTag typeTag2) throws AlgebricksException {
        byte s = AInt8SerializerDeserializer.getByte(outLeft.getByteArray(), 1);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(outRight.getByteArray(), 1);
                return compareByte(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(outRight.getByteArray(), 1);
                return compareShort(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(outRight.getByteArray(), 1);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(outRight.getByteArray(), 1);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(outRight.getByteArray(), 1);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(outRight.getByteArray(), 1);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt16 and " + typeTag2 + " .");
            }
        }
    }

    private final ComparisonResult compareByte(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareShort(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareInt(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareLong(long v1, long v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareFloat(float v1, float v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareDouble(double v1, double v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

}