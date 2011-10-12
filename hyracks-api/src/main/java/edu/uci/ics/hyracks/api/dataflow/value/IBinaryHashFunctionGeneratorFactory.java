package edu.uci.ics.hyracks.api.dataflow.value;

import java.io.Serializable;

public interface IBinaryHashFunctionGeneratorFactory  extends Serializable{
	public IBinaryHashFunction createBinaryHashFunction(int seed);
}
