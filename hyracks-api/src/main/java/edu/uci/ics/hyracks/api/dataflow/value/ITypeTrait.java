package edu.uci.ics.hyracks.api.dataflow.value;

import java.io.Serializable;

public interface ITypeTrait extends Serializable {
	public static final int VARIABLE_LENGTH = -1;	
	int getStaticallyKnownDataLength();
}
