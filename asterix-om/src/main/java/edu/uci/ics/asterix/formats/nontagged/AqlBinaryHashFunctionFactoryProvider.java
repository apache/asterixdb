package edu.uci.ics.asterix.formats.nontagged;

import java.io.Serializable;

import edu.uci.ics.asterix.dataflow.data.nontagged.hash.AObjectBinaryHashFunctionFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.hash.BooleanBinaryHashFunctionFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.hash.DoubleBinaryHashFunctionFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.hash.LongBinaryHashFunctionFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.hash.RectangleBinaryHashFunctionFactory;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;


public class AqlBinaryHashFunctionFactoryProvider implements IBinaryHashFunctionFactoryProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final AqlBinaryHashFunctionFactoryProvider INSTANCE = new AqlBinaryHashFunctionFactoryProvider();
    public static final PointableBinaryHashFunctionFactory INTEGER_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(IntegerPointable.FACTORY);
    public static final PointableBinaryHashFunctionFactory FLOAT_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(FloatPointable.FACTORY);
    public static final PointableBinaryHashFunctionFactory DOUBLE_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(DoublePointable.FACTORY);
    public static final PointableBinaryHashFunctionFactory UTF8STRING_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY);
 
    
    private AqlBinaryHashFunctionFactoryProvider() {
    }

    @Override
    public IBinaryHashFunctionFactory getBinaryHashFunctionFactory(Object type) {
        if (type == null) {
            return AObjectBinaryHashFunctionFactory.INSTANCE;
        }
        IAType aqlType = (IAType) type;
        switch (aqlType.getTypeTag()) {
            case ANY:
            case UNION: { // we could do smth better for nullable fields
                return AObjectBinaryHashFunctionFactory.INSTANCE;
            }
            case NULL: {
                return new IBinaryHashFunctionFactory() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public IBinaryHashFunction createBinaryHashFunction() {
                        return new IBinaryHashFunction() {

                            @Override
                            public int hash(byte[] bytes, int offset, int length) {
                                return 0;
                            }
                        };
                    }
                };
            }
            case BOOLEAN: {
                return addOffset(BooleanBinaryHashFunctionFactory.INSTANCE);
            }
            case INT32: {
                return addOffset(new PointableBinaryHashFunctionFactory(IntegerPointable.FACTORY));
            }
            case INT64: {
                return addOffset(LongBinaryHashFunctionFactory.INSTANCE);
            }
            case FLOAT: {
                return addOffset(new PointableBinaryHashFunctionFactory(FloatPointable.FACTORY));
            }
            case DOUBLE: {
                return addOffset(DoubleBinaryHashFunctionFactory.INSTANCE);
            }
            case STRING: {
                return addOffset(new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY));
            }
            case RECTANGLE: {
                return addOffset(RectangleBinaryHashFunctionFactory.INSTANCE);
            }
            default: {
                throw new NotImplementedException("No binary hash function factory implemented for type "
                        + aqlType.getTypeTag() + " .");
            }
        }
    }

    private IBinaryHashFunctionFactory addOffset(final IBinaryHashFunctionFactory inst) {
        return new IBinaryHashFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IBinaryHashFunction createBinaryHashFunction() {
                final IBinaryHashFunction bhf = inst.createBinaryHashFunction();
                return new IBinaryHashFunction() {

                    @Override
                    public int hash(byte[] bytes, int offset, int length) {
                        return bhf.hash(bytes, offset + 1, length);
                    }
                };
            }
        };
    }

}
