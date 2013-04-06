package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class ADateTimeAscBinaryComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    public static final ADateTimeAscBinaryComparatorFactory INSTANCE = new ADateTimeAscBinaryComparatorFactory();

    private ADateTimeAscBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

                long chrononTime1 = getLong(b1, s1);
                long chrononTime2 = getLong(b2, s2);

                if (chrononTime1 > chrononTime2) {
                    return 1;
                } else if (chrononTime1 < chrononTime2) {
                    return -1;
                } else {
                    return 0;
                }
            }

            private long getLong(byte[] bytes, int start) {
                return (((long) (bytes[start] & 0xff)) << 56) + (((long) (bytes[start + 1] & 0xff)) << 48)
                        + (((long) (bytes[start + 2] & 0xff)) << 40) + (((long) (bytes[start + 3] & 0xff)) << 32)
                        + (((long) (bytes[start + 4] & 0xff)) << 24) + (((long) (bytes[start + 5] & 0xff)) << 16)
                        + (((long) (bytes[start + 6] & 0xff)) << 8) + (((long) (bytes[start + 7] & 0xff)) << 0);
            }

        };
    }

}
