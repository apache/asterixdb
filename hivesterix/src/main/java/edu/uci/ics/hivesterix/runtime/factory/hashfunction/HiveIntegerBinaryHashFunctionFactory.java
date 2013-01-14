package edu.uci.ics.hivesterix.runtime.factory.hashfunction;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class HiveIntegerBinaryHashFunctionFactory implements
		IBinaryHashFunctionFactory {
	private static final long serialVersionUID = 1L;

	public static IBinaryHashFunctionFactory INSTANCE = new HiveIntegerBinaryHashFunctionFactory();

	private HiveIntegerBinaryHashFunctionFactory() {
	}

	@Override
	public IBinaryHashFunction createBinaryHashFunction() {

		return new IBinaryHashFunction() {
			private VInt value = new VInt();

			@Override
			public int hash(byte[] bytes, int offset, int length) {
				LazyUtils.readVInt(bytes, offset, value);
				if (value.length != length)
					throw new IllegalArgumentException(
							"length mismatch in int hash function actual: "
									+ length + " expected " + value.length);
				return value.value;
			}
		};
	}

}
