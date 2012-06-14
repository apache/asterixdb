/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.runtime.base;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilter;

public class AsterixTupleFilter implements ITupleFilter {
	
	private final IBinaryBooleanInspector boolInspector;
	private final ICopyEvaluator eval;
    private final ArrayBackedValueStorage evalOut = new ArrayBackedValueStorage();
    
	public AsterixTupleFilter(ICopyEvaluatorFactory evalFactory,
			IBinaryBooleanInspector boolInspector) throws AlgebricksException {
		this.boolInspector = boolInspector;
		eval = evalFactory.createEvaluator(evalOut);
	}
	
	@Override
	public boolean accept(IFrameTupleReference tuple) throws Exception {
		evalOut.reset();
		eval.evaluate(tuple);
		return boolInspector.getBooleanValue(evalOut.getByteArray(), 0, 2);
	}
}
