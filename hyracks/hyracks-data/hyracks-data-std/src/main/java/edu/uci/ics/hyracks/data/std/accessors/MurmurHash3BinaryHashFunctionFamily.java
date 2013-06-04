package edu.uci.ics.hyracks.data.std.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

/**
 * An implementation of the Murmur3 hash family. The code is implemented based
 * on the original <a
 * href=http://code.google.com/p/guava-libraries/source/browse
 * /guava/src/com/google/common/hash/Murmur3_32HashFunction.java>guava
 * implementation</a> from Google Guava library.
 */
public class MurmurHash3BinaryHashFunctionFamily implements
        IBinaryHashFunctionFamily {

    public static final IBinaryHashFunctionFamily INSTANCE = new MurmurHash3BinaryHashFunctionFamily();

    private static final long serialVersionUID = 1L;

    private MurmurHash3BinaryHashFunctionFamily() {
    }

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int C3 = 5;
    private static final int C4 = 0xe6546b64;
    private static final int C5 = 0x85ebca6b;
    private static final int C6 = 0xc2b2ae35;

    @Override
    public IBinaryHashFunction createBinaryHashFunction(final int seed) {
        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                int h = seed;
                int p = offset;
                int remain = length;
                while (remain >= 4) {
                    int k = (bytes[p] & 0xff) | ((bytes[p + 1] & 0xff) << 8)
                            | ((bytes[p + 2] & 0xff) << 16)
                            | ((bytes[p + 3] & 0xff) << 24);
                    k *= C1;
                    k = Integer.rotateLeft(k, 15);
                    k *= C2;
                    h ^= k;
                    h = Integer.rotateLeft(h, 13);
                    h = h * C3 + C4;
                    p += 4;
                    remain -= 4;
                }
                if (remain > 0) {
                    int k = 0;
                    for (int i = 0; remain > 0; i += 8) {
                        k ^= (bytes[p++] & 0xff) << i;
                        remain--;
                    }
                    k *= C1;
                    k = Integer.rotateLeft(k, 15);
                    k *= C2;
                    h ^= k;
                }
                h ^= length;
                h ^= (h >>> 16);
                h *= C5;
                h ^= (h >>> 13);
                h *= C6;
                h ^= (h >>> 16);
                return h;
            }
        };
    }
}