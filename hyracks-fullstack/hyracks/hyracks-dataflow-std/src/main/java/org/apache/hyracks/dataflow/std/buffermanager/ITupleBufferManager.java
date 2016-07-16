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

package org.apache.hyracks.dataflow.std.buffermanager;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Manage the buffer space. Different from the {@link IFrameBufferManager}, this one allows the record level manipulation.
 */
public interface ITupleBufferManager {
    /**
     * Reset the counters and flags to initial status. This method should not release the pre-allocated resources
     *
     * @throws org.apache.hyracks.api.exceptions.HyracksDataException
     */
    void reset() throws HyracksDataException;

    /**
     * @return the number of tuples in this buffer
     */
    int getNumTuples();

    boolean insertTuple(IFrameTupleAccessor accessor, int idx, TuplePointer tuplePointer) throws HyracksDataException;

    void close() throws HyracksDataException;

    ITuplePointerAccessor createTuplePointerAccessor();
}
