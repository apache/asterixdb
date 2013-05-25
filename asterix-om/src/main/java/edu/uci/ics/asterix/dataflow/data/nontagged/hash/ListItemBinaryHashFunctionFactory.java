package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import edu.uci.ics.asterix.formats.nontagged.UTF8StringLowercasePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class ListItemBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final ListItemBinaryHashFunctionFactory INSTANCE = new ListItemBinaryHashFunctionFactory();

    private ListItemBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
    	return createBinaryHashFunction(ATypeTag.ANY, false);
    }
    
    public IBinaryHashFunction createBinaryHashFunction(final ATypeTag itemTypeTag, final boolean ignoreCase) {
        return new IBinaryHashFunction() {

            private IBinaryHashFunction boolHash = BooleanBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
            private IBinaryHashFunction intHash = new PointableBinaryHashFunctionFactory(IntegerPointable.FACTORY)
                    .createBinaryHashFunction();
            private IBinaryHashFunction longHash = LongBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
            private IBinaryHashFunction floatHash = new PointableBinaryHashFunctionFactory(FloatPointable.FACTORY)
                    .createBinaryHashFunction();
            private IBinaryHashFunction stringHash = new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY)
                    .createBinaryHashFunction();
            private IBinaryHashFunction lowerCaseStringHash = new PointableBinaryHashFunctionFactory(UTF8StringLowercasePointable.FACTORY)
            		.createBinaryHashFunction();
            private IBinaryHashFunction doubleHash = DoubleBinaryHashFunctionFactory.INSTANCE
                    .createBinaryHashFunction();
            private IBinaryHashFunction rectangleHash = RectangleBinaryHashFunctionFactory.INSTANCE
                    .createBinaryHashFunction();
            private IBinaryHashFunction rawHash = RawBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();

            @Override
            public int hash(byte[] bytes, int offset, int length) {
            	ATypeTag tag = itemTypeTag;
            	int skip = 0;
            	if (itemTypeTag == ATypeTag.ANY) {
            		tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
            		skip = 1;
            	}
                switch (tag) {
                    case BOOLEAN: {
                        return boolHash.hash(bytes, offset + skip, length - skip);
                    }
                    case INT32: {
                        return intHash.hash(bytes, offset + skip, length - skip);
                    }
                    case INT64: {
                        return longHash.hash(bytes, offset + skip, length - skip);
                    }
                    case FLOAT: {
                        return floatHash.hash(bytes, offset + skip, length - skip);
                    }
                    case DOUBLE: {
                        return doubleHash.hash(bytes, offset + skip, length - skip);
                    }
                    case STRING: {
                    	if (ignoreCase) {
                    		return lowerCaseStringHash.hash(bytes, offset + skip, length - skip);
                    	} else {
                    		return stringHash.hash(bytes, offset + skip, length - skip);
                    	}
                    }
                    case RECTANGLE: {
                        return rectangleHash.hash(bytes, offset + skip, length - skip);
                    }
                    case NULL: {
                        return 0;
                    }
                    default: {
                        return rawHash.hash(bytes, offset + skip, length - skip);
                    }
                }
            }
        };
    }
}
