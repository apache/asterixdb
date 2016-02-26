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

package org.apache.hyracks.dataflow.std.sort.util;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Basically, it is an union of the {@link IFrameTupleAccessor} and {@link IFrameTupleAppender}.
 * Moreover, it has the delete function as well.
 * This is a special TupleAccessor used for TopK sorting.
 * In HeapSort, or other Tuple-based operators, we need to append the tuple, access the arbitrary previously
 * inserted tuple, and delete the previously inserted tuple.
 */
public interface IAppendDeletableFrameTupleAccessor extends IFrameTupleAccessor {

    /**
     * Prepare to write on this buffer
     *
     * @param buffer
     * @throws HyracksDataException
     */
    void clear(ByteBuffer buffer) throws HyracksDataException;

    /**
     * Append tuple content to this buffer. Return the new tid as a handle to the caller.
     *
     * @param tupleAccessor
     * @param tIndex
     * @return
     * @throws HyracksDataException
     */
    int append(IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException;

    /**
     * Remove the certain tuple by tid
     *
     * @param tid
     */
    void delete(int tid);

    /**
     * Reorganize the space to remove the unused space and make the free space contiguous.
     */
    void reOrganizeBuffer();

    /**
     * @return how many free space in total in the buffer, including the fragmented space
     */
    int getTotalFreeSpace();

    /**
     * @return how many contiguous free space in the buffer.
     */
    int getContiguousFreeSpace();
}
