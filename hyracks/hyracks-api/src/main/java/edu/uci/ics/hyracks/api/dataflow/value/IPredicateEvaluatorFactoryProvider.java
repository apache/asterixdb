package edu.uci.ics.hyracks.api.dataflow.value;

import java.io.Serializable;

public interface IPredicateEvaluatorFactoryProvider extends Serializable{
	public IPredicateEvaluatorFactory getPredicateEvaluatorFactory(int[] keys0, int[] keys1);
}
