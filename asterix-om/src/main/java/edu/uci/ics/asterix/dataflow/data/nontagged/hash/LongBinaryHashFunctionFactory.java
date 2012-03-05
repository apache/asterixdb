package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class LongBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final LongBinaryHashFunctionFactory INSTANCE = new LongBinaryHashFunctionFactory();

    private LongBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {

        return new IBinaryHashFunction() {

            @Override
            public int hash(byte[] bytes, int offset, int length) {
                return IntegerSerializerDeserializer.getInt(bytes, offset + 4);
            }
        };
    }

}
