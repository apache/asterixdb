package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;

public class AObjectBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final AObjectBinaryHashFunctionFactory INSTANCE = new AObjectBinaryHashFunctionFactory();

    private AObjectBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        return new IBinaryHashFunction() {
            private IBinaryHashFunction genericBinaryHash = MurmurHash3BinaryHashFunctionFamily.INSTANCE
                    .createBinaryHashFunction(0);

            @Override
            public int hash(byte[] bytes, int offset, int length) {
                return genericBinaryHash.hash(bytes, offset, length);
            }
        };
    }
}
