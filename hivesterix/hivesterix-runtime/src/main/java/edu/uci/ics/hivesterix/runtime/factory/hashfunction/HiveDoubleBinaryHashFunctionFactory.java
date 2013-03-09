package edu.uci.ics.hivesterix.runtime.factory.hashfunction;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class HiveDoubleBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {
    private static final long serialVersionUID = 1L;

    public static HiveDoubleBinaryHashFunctionFactory INSTANCE = new HiveDoubleBinaryHashFunctionFactory();

    private HiveDoubleBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        // TODO Auto-generated method stub
        return new IBinaryHashFunction() {
            private Double value;

            @Override
            public int hash(byte[] bytes, int offset, int length) {
                value = Double.longBitsToDouble(LazyUtils.byteArrayToLong(bytes, offset));
                return value.hashCode();
            }
        };
    }

}
