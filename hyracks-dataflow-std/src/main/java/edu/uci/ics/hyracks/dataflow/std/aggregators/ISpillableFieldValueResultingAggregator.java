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
package edu.uci.ics.hyracks.dataflow.std.aggregators;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * An extended version of the {@link IFieldValueResultingAggregator} supporting
 * external aggregation.
 * 
 */
public interface ISpillableFieldValueResultingAggregator extends
		IFieldValueResultingAggregator {

	/**
	 * Called once per aggregator before calling accumulate for the first time.
	 * 
	 * @param accessor
	 *            - Accessor to the data tuple.
	 * @param tIndex
	 *            - Index of the tuple in the accessor.
	 * @throws HyracksDataException
	 */
	public void initFromPartial(IFrameTupleAccessor accessor, int tIndex,
			int fIndex) throws HyracksDataException;

	/**
	 * Aggregate another partial result.
	 * 
	 * @param accessor
	 * @param tIndex
	 * @param fIndex
	 * @throws HyracksDataException
	 */
	public void accumulatePartialResult(IFrameTupleAccessor accessor,
			int tIndex, int fIndex) throws HyracksDataException;

}
