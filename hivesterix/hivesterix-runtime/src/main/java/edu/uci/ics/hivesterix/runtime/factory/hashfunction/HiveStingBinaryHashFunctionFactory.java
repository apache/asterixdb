package edu.uci.ics.hivesterix.runtime.factory.hashfunction;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class HiveStingBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {
    private static final long serialVersionUID = 1L;

    public static HiveStingBinaryHashFunctionFactory INSTANCE = new HiveStingBinaryHashFunctionFactory();

    private HiveStingBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        // TODO Auto-generated method stub
        return new IBinaryHashFunction() {
            private VInt len = new VInt();

            @Override
            public int hash(byte[] bytes, int offset, int length) {
                LazyUtils.readVInt(bytes, offset, len);
                if (len.value + len.length != length)
                    throw new IllegalStateException("parse string: length mismatch, expected "
                            + (len.value + len.length) + " but get " + length);
                return hashBytes(bytes, offset + len.length, length - len.length);
            }

            public int hashBytes(byte[] bytes, int offset, int length) {
                int value = 1;
                int end = offset + length;
                for (int i = offset; i < end; i++)
                    value = value * 31 + (int) bytes[i];
                return value;
            }
        };
    }

}
