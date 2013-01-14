package edu.uci.ics.hivesterix.runtime.factory.normalize;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class HiveDoubleDescNormalizedKeyComputerFactory implements
		INormalizedKeyComputerFactory {

	private static final long serialVersionUID = 1L;
	private final INormalizedKeyComputerFactory ascNormalizedKeyComputerFactory = new HiveDoubleAscNormalizedKeyComputerFactory();

	@Override
	public INormalizedKeyComputer createNormalizedKeyComputer() {
		return new INormalizedKeyComputer() {
			private INormalizedKeyComputer nmkComputer = ascNormalizedKeyComputerFactory
					.createNormalizedKeyComputer();

			@Override
			public int normalize(byte[] bytes, int start, int length) {
				int nk = nmkComputer.normalize(bytes, start, length);
				return (int) ((long) Integer.MAX_VALUE - (long) (nk - Integer.MIN_VALUE));
			}

		};
	}
}
