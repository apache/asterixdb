package edu.uci.ics.asterix.om.types;

import edu.uci.ics.asterix.om.base.IAObject;

public abstract class AbstractComplexType implements IAType {

	private static final long serialVersionUID = 1L;
	protected String typeName;

	public AbstractComplexType(String typeName) {
		this.typeName = typeName;
	}

	@Override
	public String getTypeName() {
		return typeName;
	}

	@Override
	public boolean equals(Object object) {
		return this.deepEqual((IAObject) object);
	}

}
