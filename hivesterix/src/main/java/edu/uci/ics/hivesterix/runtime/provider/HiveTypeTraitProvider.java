package edu.uci.ics.hivesterix.runtime.provider;

import java.io.Serializable;

import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class HiveTypeTraitProvider implements ITypeTraitProvider, Serializable {
	private static final long serialVersionUID = 1L;
	public static HiveTypeTraitProvider INSTANCE = new HiveTypeTraitProvider();

	private HiveTypeTraitProvider() {

	}

	@Override
	public ITypeTraits getTypeTrait(Object arg0) {
		return new ITypeTraits() {
			private static final long serialVersionUID = 1L;

			@Override
			public int getFixedLength() {
				return -1;
			}

			@Override
			public boolean isFixedLength() {
				return false;
			}

		};
	}
}
