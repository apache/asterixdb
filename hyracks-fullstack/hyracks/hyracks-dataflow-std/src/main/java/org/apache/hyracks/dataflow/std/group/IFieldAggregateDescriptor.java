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

import java.io.DataOutput;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 *
 */
public interface IFieldAggregateDescriptor {

    /**
     * Initialize the state based on the input tuple.
     *
     * @param accessor
     * @param tIndex
     * @param fieldOutput
     *            The data output for the frame containing the state. This may
     *            be null, if the state is maintained as a java object.
     *
     *            Note that we have an assumption that the initialization of the
     *            binary state (if any) inserts the state fields into the buffer
     *            in a appending fashion. This means that an arbitrary initial
     *            size of the state can be accquired.
     * @param state
     *            The state to be initialized.
     * @throws HyracksDataException
     */
    public void init(IFrameTupleAccessor accessor, int tIndex, DataOutput fieldOutput, AggregateState state)
            throws HyracksDataException;

    /**
     * Reset the aggregator. The corresponding aggregate state should be reset
     * too. Note that here the frame is not an input argument, since it can be
     * reset outside of the aggregator (simply reset the starting index of the
     * buffer).
     *
     * @param state
     */
    public void reset();

    /**
     * Aggregate the value. Aggregate state should be updated correspondingly.
     *
     * @param accessor
     * @param tIndex
     * @param data
     *            The buffer containing the state, if frame-based-state is used.
     *            This means that it can be null if java-object-based-state is
     *            used.
     *
     *            Here the length of binary state can be obtains from the state
     *            parameter, and if the content to be filled into that is over-
     *            flowing (larger than the reversed space), error should be
     *            emit.
     * @param offset
     * @param state
     *            The aggregate state.
     * @throws HyracksDataException
     */
    public void aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, AggregateState state)
            throws HyracksDataException;

    /**
     * Output the partial aggregation result.
     *
     * @param fieldOutput
     *            The data output for the output frame
     * @param data
     *            The buffer containing the aggregation state
     * @param offset
     * @param state
     *            The aggregation state.
     * @throws HyracksDataException
     */
    public void outputPartialResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
            throws HyracksDataException;

    /**
     * Output the final aggregation result.
     *
     * @param fieldOutput
     *            The data output for the output frame
     * @param data
     *            The buffer containing the aggregation state
     * @param offset
     * @param state
     *            The aggregation state.
     * @throws HyracksDataException
     */
    public void outputFinalResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
            throws HyracksDataException;

    public boolean needsBinaryState();

    public boolean needsObjectState();

    public AggregateState createState();

    /**
     * Close the field aggregator
     */
    public void close();

}
