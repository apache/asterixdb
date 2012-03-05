package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class LongBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final LongBinaryComparatorFactory INSTANCE = new LongBinaryComparatorFactory();

    private LongBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                long v1 = AInt64SerializerDeserializer.getLong(b1, s1);
                long v2 = AInt64SerializerDeserializer.getLong(b2, s2);
                return v1 < v2 ? -1 : (v1 > v2 ? 1 : 0);
            }
        };
    }

}
