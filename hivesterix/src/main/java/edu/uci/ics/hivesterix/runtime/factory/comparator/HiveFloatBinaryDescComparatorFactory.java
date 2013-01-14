package edu.uci.ics.hivesterix.runtime.factory.comparator;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class HiveFloatBinaryDescComparatorFactory implements
		IBinaryComparatorFactory {
	private static final long serialVersionUID = 1L;

	public static HiveFloatBinaryDescComparatorFactory INSTANCE = new HiveFloatBinaryDescComparatorFactory();

	private HiveFloatBinaryDescComparatorFactory() {
	}

	@Override
	public IBinaryComparator createBinaryComparator() {
		return new IBinaryComparator() {
			private float left;
			private float right;

			@Override
			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
					int l2) {
				left = Float.intBitsToFloat(LazyUtils.byteArrayToInt(b1, s1));
				right = Float.intBitsToFloat(LazyUtils.byteArrayToInt(b2, s2));
				if (left > right)
					return -1;
				else if (left == right)
					return 0;
				else
					return 1;
			}
		};
	}

}
