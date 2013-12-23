package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;

public class AUUIDPartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final AUUIDPartialBinaryComparatorFactory INSTANCE = new AUUIDPartialBinaryComparatorFactory();

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                int msbCompare = Long.compare(Integer64SerializerDeserializer.getLong(b1, s1),
                        Integer64SerializerDeserializer.getLong(b2, s2));
                if (msbCompare == 0) {
                    return Long.compare(Integer64SerializerDeserializer.getLong(b1, s1 + 8),
                            Integer64SerializerDeserializer.getLong(b2, s2 + 8));
                } else {
                    return msbCompare;
                }
            }
        };
    }

}
