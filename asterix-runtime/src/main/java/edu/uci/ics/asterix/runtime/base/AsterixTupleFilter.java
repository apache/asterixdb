package edu.uci.ics.asterix.runtime.base;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilter;

public class AsterixTupleFilter implements ITupleFilter {
	
	private final IBinaryBooleanInspector boolInspector;
	private final IEvaluator eval;
    private final ArrayBackedValueStorage evalOut = new ArrayBackedValueStorage();
    
	public AsterixTupleFilter(IEvaluatorFactory evalFactory,
			IBinaryBooleanInspector boolInspector) throws AlgebricksException {
		this.boolInspector = boolInspector;
		eval = evalFactory.createEvaluator(evalOut);
	}
	
	@Override
	public boolean accept(IFrameTupleReference tuple) throws Exception {
		evalOut.reset();
		eval.evaluate(tuple);
		return boolInspector.getBooleanValue(evalOut.getBytes(), 0, 2);
	}
}
