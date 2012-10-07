package edu.uci.ics.pregelix.core.jobgen;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.WritableComparingBinaryComparatorFactory;
import edu.uci.ics.pregelix.core.jobgen.provider.NormalizedKeyComputerFactoryProvider;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class JobGenUtil {

    /**
     * get normalized key factory for an iteration, to sort messages iteration
     * 1: asc order iteration 2: desc order
     * 
     * @param iteration
     * @param keyClass
     * @return
     */
    public static INormalizedKeyComputerFactory getINormalizedKeyComputerFactory(int iteration, Class keyClass) {
        return NormalizedKeyComputerFactoryProvider.INSTANCE.getAscINormalizedKeyComputerFactory(keyClass);
    }

    /**
     * get comparator for an iteration, to sort messages iteration 1: asc order
     * iteration 0: desc order
     * 
     * @param iteration
     * @param keyClass
     * @return
     */
    public static IBinaryComparatorFactory getIBinaryComparatorFactory(int iteration, Class keyClass) {
        return new WritableComparingBinaryComparatorFactory(keyClass);
    }

    /**
     * get the B-tree scan order for an iteration iteration 1: desc order,
     * backward scan iteration 2: asc order, forward scan
     * 
     * @param iteration
     * @return
     */
    public static boolean getForwardScan(int iteration) {
        return true;
    }

}
