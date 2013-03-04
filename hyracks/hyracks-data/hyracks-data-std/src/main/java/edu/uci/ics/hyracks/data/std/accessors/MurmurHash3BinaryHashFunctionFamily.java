package edu.uci.ics.hyracks.data.std.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

public class MurmurHash3BinaryHashFunctionFamily implements IBinaryHashFunctionFamily {
    private static final long serialVersionUID = 1L;

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
                while (remain > 4) {
                    int k = ((int) bytes[p]) | (((int) bytes[p + 1]) << 8) | (((int) bytes[p + 2]) << 16)
                            | (((int) bytes[p + 3]) << 24);
                    k *= C1;
                    k = Integer.rotateLeft(k, 15);
                    k *= C2;
                    h ^= k;
                    h = Integer.rotateLeft(h, 13);
                    h = h * C3 + C4;
                    p += 4;
                    remain -= 4;
                }
                int k = 0;
                switch (remain) {
                    case 3:
                        k = bytes[p++];
                    case 2:
                        k = (k << 8) | bytes[p++];
                    case 1:
                        k = (k << 8) | bytes[p++];
                        k *= C1;
                        k = Integer.rotateLeft(k, 15);
                        k *= C2;
                        h ^= k;
                        h = Integer.rotateLeft(h, 13);
                        h = h * C3 + C4;
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