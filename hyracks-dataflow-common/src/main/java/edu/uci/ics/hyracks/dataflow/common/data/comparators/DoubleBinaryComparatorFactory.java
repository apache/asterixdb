package edu.uci.ics.hyracks.dataflow.common.data.comparators;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;

public class DoubleBinaryComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    public static final DoubleBinaryComparatorFactory INSTANCE = new DoubleBinaryComparatorFactory();

    private DoubleBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {
            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {   
                return Double.compare(DoubleSerializerDeserializer.getDouble(b1, s1), DoubleSerializerDeserializer
                        .getDouble(b2, s2));
            }
        };
    }
}