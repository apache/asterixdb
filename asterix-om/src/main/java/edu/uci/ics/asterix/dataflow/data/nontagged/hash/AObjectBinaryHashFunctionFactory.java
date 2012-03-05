package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class AObjectBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final AObjectBinaryHashFunctionFactory INSTANCE = new AObjectBinaryHashFunctionFactory();

    private AObjectBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        return new IBinaryHashFunction() {

            private IBinaryHashFunction boolHash = BooleanBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
            private IBinaryHashFunction intHash = new PointableBinaryHashFunctionFactory(IntegerPointable.FACTORY)
                    .createBinaryHashFunction();
            private IBinaryHashFunction longHash = LongBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
            private IBinaryHashFunction floatHash = new PointableBinaryHashFunctionFactory(FloatPointable.FACTORY)
                    .createBinaryHashFunction();
            private IBinaryHashFunction stringHash = new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY)
                    .createBinaryHashFunction();

            private IBinaryHashFunction doubleHash = DoubleBinaryHashFunctionFactory.INSTANCE
                    .createBinaryHashFunction();
            private IBinaryHashFunction rectangleHash = RectangleBinaryHashFunctionFactory.INSTANCE
                    .createBinaryHashFunction();

            @Override
            public int hash(byte[] bytes, int offset, int length) {
                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
                switch (tag) {
                    case BOOLEAN: {
                        return boolHash.hash(bytes, offset + 1, length - 1);
                    }
                    case INT32: {
                        return intHash.hash(bytes, offset + 1, length - 1);
                    }
                    case INT64: {
                        return longHash.hash(bytes, offset + 1, length - 1);
                    }
                    case FLOAT: {
                        return floatHash.hash(bytes, offset + 1, length - 1);
                    }
                    case DOUBLE: {
                        return doubleHash.hash(bytes, offset + 1, length - 1);
                    }
                    case STRING: {
                        return stringHash.hash(bytes, offset + 1, length - 1);
                    }
                    case RECTANGLE: {
                        return rectangleHash.hash(bytes, offset + 1, length - 1);
                    }
                    case NULL: {
                        return 0;
                    }
                    default: {
                        throw new NotImplementedException("Binary hashing for the " + tag + " type is not implemented.");
                    }
                }
            }
        };
    }
}
