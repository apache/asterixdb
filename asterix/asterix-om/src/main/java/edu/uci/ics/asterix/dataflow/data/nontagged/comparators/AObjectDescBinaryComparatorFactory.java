package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class AObjectDescBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final IBinaryComparatorFactory INSTANCE = new AObjectDescBinaryComparatorFactory();

    private AObjectDescBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {
            final IBinaryComparator ascComp = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return -ascComp.compare(b1, s1, l1, b2, s2, l2);
            }
        };
    }

}
