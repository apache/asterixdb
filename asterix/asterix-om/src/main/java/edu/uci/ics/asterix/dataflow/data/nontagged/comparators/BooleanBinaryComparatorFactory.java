package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class BooleanBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final BooleanBinaryComparatorFactory INSTANCE = new BooleanBinaryComparatorFactory();

    private BooleanBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                boolean v1 = ABooleanSerializerDeserializer.getBoolean(b1, s1);
                boolean v2 = ABooleanSerializerDeserializer.getBoolean(b2, s2);
                if (v1) {
                    return v2 ? 0 : 1;
                } else {
                    return v2 ? -1 : 0;
                }
            }
        };
    }

}
