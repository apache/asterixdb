package edu.uci.ics.hyracks.hdfs.lib;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class RawBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    public static IBinaryComparatorFactory INSTANCE = new RawBinaryComparatorFactory();

    private RawBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                int commonLength = Math.min(l1, l2);
                for (int i = 0; i < commonLength; i++) {
                    if (b1[s1 + i] != b2[s2 + i]) {
                        return b1[s1 + i] - b2[s2 + i];
                    }
                }
                int difference = l1 - l2;
                return difference == 0 ? 0 : (difference > 0 ? 1 : -1);
            }

        };
    }
}
