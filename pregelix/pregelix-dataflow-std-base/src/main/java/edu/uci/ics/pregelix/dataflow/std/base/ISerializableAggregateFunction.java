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
package edu.uci.ics.pregelix.dataflow.std.base;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public interface ISerializableAggregateFunction {
    /** should be called each time a new aggregate value is computed */
    public void init(IFrameTupleReference tuple, ArrayTupleBuilder state) throws HyracksDataException;

    public void step(IFrameTupleReference tuple, IFrameTupleReference state) throws HyracksDataException;

    public void finishPartial(IFrameTupleReference state, ArrayTupleBuilder output) throws HyracksDataException;

    public void finishFinal(IFrameTupleReference state, ArrayTupleBuilder output) throws HyracksDataException;
}
