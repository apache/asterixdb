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
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface IAggregatorDescriptor {

    /**
     * Initialize the aggregator with an input tuple specified by the input
     * frame and tuple index. This function will write the initialized partial
     * result into the tuple builder.
     * 
     * @param accessor
     * @param tIndex
     * @param tupleBuilder
     * @throws HyracksDataException
     */
    public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException;

    /**
     * Aggregate the input tuple with the partial result specified by the bytes.
     * The new value then is written back to the bytes field specified.
     * It is the developer's responsibility to have the new result not exceed
     * the given bytes.
     * 
     * @param accessor
     * @param tIndex
     * @param data
     * @param offset
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public int aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length)
            throws HyracksDataException;

    /**
     * Output the partial aggregation result to an array tuple builder.
     * Necessary additional information for aggregation should be maintained.
     * For example, for an aggregator calculating AVG, the count and also the
     * current average should be maintained as the partial results.
     * 
     * @param accessor
     * @param tIndex
     * @param tupleBuilder
     * @throws HyracksDataException
     */
    public void outputPartialResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException;

    /**
     * Output the final aggregation result to an array tuple builder.
     * 
     * @param accessor
     * @param tIndex
     * @param tupleBuilder
     * @return
     * @throws HyracksDataException
     */
    public void outputResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException;

    /**
     * reset the internal states
     */
    public void reset();

    /**
     * Close the aggregator. Necessary clean-up code should be implemented here.
     */
    public void close();

}
