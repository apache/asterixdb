package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class DoubleBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final DoubleBinaryHashFunctionFactory INSTANCE = new DoubleBinaryHashFunctionFactory();

    private DoubleBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                // copied from Double.hashCode()
                long bits = ADoubleSerializerDeserializer.getLongBits(bytes, offset);
                return (int) (bits ^ (bits >>> 32));
            }
        };
    }

}
