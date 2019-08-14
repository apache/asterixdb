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

package org.apache.hyracks.api.dataflow.value;

import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ITupleMultiPartitionComputer {
    /**
     * For the tuple (located at tIndex in the frame), it determines which target partitions (0,1,... nParts-1) the
     * tuple should be sent/written to.
     * @param accessor The accessor of the frame to access tuples
     * @param tIndex The index of the tuple in consideration
     * @param nParts The number of target partitions
     * @return The chosen target partitions as dictated by the logic of the partition computer
     * @throws HyracksDataException
     */
    BitSet partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException;

    /**
     * Gives the data partitioner a chance to set up its environment before it starts partitioning tuples. This method
     * should be called in the open() of {@link org.apache.hyracks.api.comm.IFrameWriter}. The default implementation
     * is "do nothing".
     * @throws HyracksDataException
     */
    default void initialize() throws HyracksDataException {
    }
}
