package edu.uci.ics.hivesterix.runtime.factory.normalize;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class HiveDoubleAscNormalizedKeyComputerFactory implements
		INormalizedKeyComputerFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public INormalizedKeyComputer createNormalizedKeyComputer() {

		return new INormalizedKeyComputer() {

			@Override
			public int normalize(byte[] bytes, int start, int length) {
				int header = LazyUtils.byteArrayToInt(bytes, start);
				long unsignedValue = (long) header;
				return (int) ((unsignedValue - ((long) Integer.MIN_VALUE)) & 0xffffffffL);
			}
		};
	}
}
