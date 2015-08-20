/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.std.sort.buffermanager;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IFramePool {

    int getMinFrameSize();

    int getMemoryBudgetBytes();

    /**
     * Get a frame of given size. <br>
     * Returns {@code null} if failed to allocate the required size of frame
     *
     * @param frameSize the actual size of the frame.
     * @return the allocated frame
     * @throws HyracksDataException
     */
    ByteBuffer allocateFrame(int frameSize) throws HyracksDataException;

    /**
     * Reset the counters to initial status. This method should not release the pre-allocated resources.
     */
    void reset();

    /**
     * Release the pre-allocated resources.
     */
    void close();

}
