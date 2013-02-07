package edu.uci.ics.hivesterix.runtime.jobgen;

import edu.uci.ics.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class HiveDomain implements INodeDomain {

	@Override
	public boolean sameAs(INodeDomain domain) {
		return true;
	}

	@Override
	public Integer cardinality() {
		return 0;
	}

}
