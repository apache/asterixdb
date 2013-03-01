package edu.uci.ics.hivesterix.runtime.factory.hashfunction;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class HiveRawBinaryHashFunctionFactory implements
		IBinaryHashFunctionFactory {
	private static final long serialVersionUID = 1L;

	public static IBinaryHashFunctionFactory INSTANCE = new HiveRawBinaryHashFunctionFactory();

	private HiveRawBinaryHashFunctionFactory() {

	}

	@Override
	public IBinaryHashFunction createBinaryHashFunction() {

		return new IBinaryHashFunction() {

			@Override
			public int hash(byte[] bytes, int offset, int length) {
				int value = 1;
				int end = offset + length;
				for (int i = offset; i < end; i++)
					value = value * 31 + (int) bytes[i];
				return value;
			}
		};
	}

}
