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

package org.apache.hyracks.dataflow.common.comm.io;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleReversibleAppender;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IntSerDeUtils;

public class FixedSizeFrameTupleAppender extends FrameTupleAppender implements IFrameTupleReversibleAppender {
    @Override
    protected boolean canHoldNewTuple(int fieldCount, int dataLength) throws HyracksDataException {
        if (hasEnoughSpace(fieldCount, dataLength)) {
            return true;
        }
        return false;
    }

    /**
     * Cancels the lastly performed append operation. i.e. decreases the tuple count and resets the data end offset.
     */
    @Override
    public boolean cancelAppend() throws HyracksDataException {
        // Decreases tupleCount by one.
        tupleCount = IntSerDeUtils.getInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()));
        if (tupleCount == 0) {
            // There is no inserted tuple in the given frame. This should not happen.
            return false;
        }
        tupleCount = tupleCount - 1;

        // Resets tupleCount and DataEndOffset.
        IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
        tupleDataEndOffset = tupleCount == 0 ? FrameConstants.TUPLE_START_OFFSET
                : IntSerDeUtils.getInt(array,
                        FrameHelper.getTupleCountOffset(frame.getFrameSize()) - tupleCount * FrameConstants.SIZE_LEN);
        return true;
    }

}