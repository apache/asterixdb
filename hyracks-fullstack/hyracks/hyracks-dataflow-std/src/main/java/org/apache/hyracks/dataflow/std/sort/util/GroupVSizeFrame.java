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

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class GroupVSizeFrame extends VSizeFrame {

    public GroupVSizeFrame(IHyracksCommonContext ctx, int frameSize) throws HyracksDataException {
        super(ctx, frameSize);
    }

    @Override
    public void reset() throws HyracksDataException {
        if (buffer.position() > 0 && buffer.hasRemaining()) {
            movePartialFutureToStartPosition();
        } else {
            buffer.clear();
        }
    }

    private void movePartialFutureToStartPosition() {
        assert buffer.hasArray();
        if (!FrameHelper.hasBeenCleared(buffer, buffer.position())) {
            buffer.compact();
            FrameHelper.clearRemainingFrame(buffer, buffer.position()); // mark it to make reset idempotent
        }
    }
}
