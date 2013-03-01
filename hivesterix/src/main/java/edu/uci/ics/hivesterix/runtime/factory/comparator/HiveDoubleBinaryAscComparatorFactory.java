package edu.uci.ics.hivesterix.runtime.factory.comparator;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class HiveDoubleBinaryAscComparatorFactory implements
		IBinaryComparatorFactory {
	private static final long serialVersionUID = 1L;

	public static HiveDoubleBinaryAscComparatorFactory INSTANCE = new HiveDoubleBinaryAscComparatorFactory();

	private HiveDoubleBinaryAscComparatorFactory() {
	}

	@Override
	public IBinaryComparator createBinaryComparator() {
		return new IBinaryComparator() {
			private double left;
			private double right;

			@Override
			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
					int l2) {
				left = Double.longBitsToDouble(LazyUtils
						.byteArrayToLong(b1, s1));
				right = Double.longBitsToDouble(LazyUtils.byteArrayToLong(b2,
						s2));
				if (left > right)
					return 1;
				else if (left == right)
					return 0;
				else
					return -1;
			}
		};
	}

}
