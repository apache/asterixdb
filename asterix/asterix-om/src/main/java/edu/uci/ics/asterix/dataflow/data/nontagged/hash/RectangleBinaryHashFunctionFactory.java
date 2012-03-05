package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class RectangleBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final RectangleBinaryHashFunctionFactory INSTANCE = new RectangleBinaryHashFunctionFactory();

    private RectangleBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                long xBits = ADoubleSerializerDeserializer.getLongBits(bytes, offset);
                long yBits = ADoubleSerializerDeserializer.getLongBits(bytes, offset + 8);
                return (int) ((xBits ^ (xBits >>> 32)) ^ (yBits ^ (yBits >>> 32)));
            }
        };
    }

}