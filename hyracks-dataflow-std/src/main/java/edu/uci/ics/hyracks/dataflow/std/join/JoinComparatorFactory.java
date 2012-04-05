package edu.uci.ics.hyracks.dataflow.std.join;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;


public class JoinComparatorFactory implements ITuplePairComparatorFactory {
    private static final long serialVersionUID = 1L;

    private final IBinaryComparatorFactory bFactory;
    private final int pos0;
    private final int pos1;

    public JoinComparatorFactory(IBinaryComparatorFactory bFactory, int pos0, int pos1) {
        this.bFactory = bFactory;
        this.pos0 = pos0;
        this.pos1 = pos1;
    }

    @Override
    public ITuplePairComparator createTuplePairComparator() {
        return new JoinComparator(bFactory.createBinaryComparator(), pos0, pos1);
    }

}
