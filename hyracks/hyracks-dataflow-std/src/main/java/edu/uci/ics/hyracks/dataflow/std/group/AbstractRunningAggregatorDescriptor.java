/*
 * Copyright 2009-2013 by The Regents of the University of California
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

public abstract class AbstractRunningAggregatorDescriptor implements IAggregatorDescriptor {

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor#outputPartialResult(edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder, edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor, int, edu.uci.ics.hyracks.dataflow.std.group.AggregateState)
     */
    @Override
    public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
            AggregateState state) throws HyracksDataException {
        return false;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor#outputFinalResult(edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder, edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor, int, edu.uci.ics.hyracks.dataflow.std.group.AggregateState)
     */
    @Override
    public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
            AggregateState state) throws HyracksDataException {
        return false;
    }

}
