package edu.uci.ics.hivesterix.runtime.factory.comparator;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class HiveIntegerBinaryAscComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    public static final HiveIntegerBinaryAscComparatorFactory INSTANCE = new HiveIntegerBinaryAscComparatorFactory();

    private HiveIntegerBinaryAscComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {
            private VInt left = new VInt();
            private VInt right = new VInt();

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                LazyUtils.readVInt(b1, s1, left);
                LazyUtils.readVInt(b2, s2, right);

                if (left.length != l1 || right.length != l2)
                    throw new IllegalArgumentException("length mismatch in int comparator function actual: "
                            + left.length + "," + right.length + " expected " + l1 + "," + l2);

                if (left.value > right.value)
                    return 1;
                else if (left.value == right.value)
                    return 0;
                else
                    return -1;
            }
        };
    }
}
