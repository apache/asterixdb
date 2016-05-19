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

package org.apache.hyracks.storage.am.lsm.common.api;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ILSMIndexFrameWriter extends IFrameWriter {
    /**
     * Push tuples in the frame from startTupleIndex(inclusive) to endTupleIndex(exclusive)
     * forward to the next operator/consumer.
     *
     * @param startTupleIndex
     *            the first tuple's index to be pushed
     * @param endTupleIndex
     *            the last tuple's index to be pushed
     * @throws HyracksDataException
     */
    public default void flushPartialFrame(int startTupleIndex, int endTupleIndex) throws HyracksDataException {
        throw new HyracksDataException("flushPartialFrame() is not supported in this ILSMIndexFrameWriter");
    }

    /**
     * Push tuples in the frame forward to the next operator/consumer.
     * The flushed tuples don't have to be all tuples in the frame.
     *
     * @throws HyracksDataException
     */
    public default void flushPartialFrame() throws HyracksDataException {
        throw new HyracksDataException("flushPartialFrame() is not supported in this ILSMIndexFrameWriter");
    }
}
