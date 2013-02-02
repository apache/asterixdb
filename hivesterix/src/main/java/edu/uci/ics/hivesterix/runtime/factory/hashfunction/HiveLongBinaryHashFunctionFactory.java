package edu.uci.ics.hivesterix.runtime.factory.hashfunction;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VLong;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class HiveLongBinaryHashFunctionFactory implements
		IBinaryHashFunctionFactory {
	private static final long serialVersionUID = 1L;

	public static IBinaryHashFunctionFactory INSTANCE = new HiveLongBinaryHashFunctionFactory();

	private HiveLongBinaryHashFunctionFactory() {
	}

	@Override
	public IBinaryHashFunction createBinaryHashFunction() {

		return new IBinaryHashFunction() {
			private VLong value = new VLong();

			@Override
			public int hash(byte[] bytes, int offset, int length) {
				LazyUtils.readVLong(bytes, offset, value);
				return (int) value.value;
			}
		};
	}

}
