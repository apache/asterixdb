package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;

public class BooleanBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final BooleanBinaryHashFunctionFactory INSTANCE = new BooleanBinaryHashFunctionFactory();

    private BooleanBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        return new IBinaryHashFunction() {

            @Override
            public int hash(byte[] bytes, int offset, int length) {
                boolean v = BooleanSerializerDeserializer.getBoolean(bytes, offset);
                return v ? 1 : 0;
            }
        };
    }

}
