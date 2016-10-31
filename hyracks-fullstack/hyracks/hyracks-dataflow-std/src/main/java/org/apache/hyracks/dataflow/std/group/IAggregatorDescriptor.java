/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.group;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface IAggregatorDescriptor {

    /**
     * Create an aggregate state
     *
     * @return an aggregate state
     * @throws HyracksDataException
     */
    AggregateState createAggregateStates() throws HyracksDataException;

    /**
     * Initialize the state based on the input tuple.
     *
     * @param tupleBuilder
     * @param accessor
     * @param tIndex
     * @param state
     * @throws HyracksDataException
     */
    void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex, AggregateState state)
            throws HyracksDataException;

    /**
     * Reset the aggregator. The corresponding aggregate state should be reset
     * too. Note that here the frame is not an input argument, since it can be
     * reset outside of the aggregator (simply reset the starting index of the
     * buffer).
     */
    void reset();

    /**
     * Aggregate the value. Aggregate state should be updated correspondingly.
     *
     * @param accessor
     * @param tIndex
     * @param stateAccessor
     *            The buffer containing the state, if frame-based-state is used.
     *            This means that it can be null if java-object-based-state is
     *            used.
     * @param stateTupleIndex
     * @param state
     *            The aggregate state.
     * @throws HyracksDataException
     */
    void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor, int stateTupleIndex,
            AggregateState state) throws HyracksDataException;

    /**
     * Output the partial aggregation result.
     *
     * @param tupleBuilder
     *            The data output for the output aggregation result
     * @param stateAccessor
     *            The stateAccessor buffer containing the aggregation state
     * @param tIndex
     * @param state
     *            The aggregation state.
     * @return true if it has any output writed to {@code tupleBuilder}
     * @throws HyracksDataException
     */
    boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor, int tIndex,
            AggregateState state) throws HyracksDataException;

    /**
     * Output the final aggregation result.
     *
     * @param tupleBuilder
     *            The data output for the output frame
     * @param stateAccessor
     *            The buffer containing the aggregation state
     * @param tIndex
     * @param state
     *            The aggregation state.
     * @return true if it has any output writed to {@code tupleBuilder}
     * @throws HyracksDataException
     */
    boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor, int tIndex,
            AggregateState state) throws HyracksDataException;

    void close();

}
