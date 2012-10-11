package edu.uci.ics.pregelix.core.runtime.touchpoint;

import org.apache.hadoop.io.RawComparator;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.util.ReflectionUtils;

public class WritableComparingBinaryComparatorFactory<T> implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    private Class<? extends RawComparator<T>> cmpClass;

    public WritableComparingBinaryComparatorFactory(Class<? extends RawComparator<T>> cmpClass) {
        this.cmpClass = cmpClass;
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        final RawComparator<T> instance = ReflectionUtils.createInstance(cmpClass);
        return new IBinaryComparator() {
            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return instance.compare(b1, s1, l1, b2, s2, l2);
            }
        };
    }
}