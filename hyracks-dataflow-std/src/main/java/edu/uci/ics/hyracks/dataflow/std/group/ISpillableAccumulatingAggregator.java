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
package edu.uci.ics.hyracks.dataflow.std.group;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * An spillable version of the {@link IAccumulatingAggregator} supporting
 * external aggregation.
 * 
 */
public interface ISpillableAccumulatingAggregator extends
		IAccumulatingAggregator {

	public void initFromPartial(IFrameTupleAccessor accessor, int tIndex,
			int[] keyFieldIndexes) throws HyracksDataException;

	/**
	 * 
	 * @param accessor
	 * @param tIndex
	 * @throws HyracksDataException
	 */
	public void accumulatePartialResult(IFrameTupleAccessor accessor,
			int tIndex, int[] keyFieldIndexes) throws HyracksDataException;

	public boolean output(FrameTupleAppender appender, ArrayTupleBuilder tbder)
			throws HyracksDataException;
}
