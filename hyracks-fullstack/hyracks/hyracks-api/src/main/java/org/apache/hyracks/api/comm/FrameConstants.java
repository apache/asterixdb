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
package org.apache.hyracks.api.comm;

public interface FrameConstants {
    /**
     * We use 4bytes to store the tupleCount at the end of the Frame.
     */
    int SIZE_LEN = 4;

    /**
     * The offset of the frame_count which is one int indicates how many initial_frames contained in current frame.
     * The actual frameSize = frame_count * initialFrameSize(given by user)
     */
    int META_DATA_FRAME_COUNT_OFFSET = 0;

    /**
     * The start offset of the tuple data. The first int is used to store the frame_count
     */
    int TUPLE_START_OFFSET = 5;

    /**
     * The max frame size is Integer.MAX_VALUE.
     */
    int MAX_FRAMESIZE = Integer.MAX_VALUE;

    /**
     * Indicate the total size of the meta data.
     */
    int META_DATA_LEN = SIZE_LEN + TUPLE_START_OFFSET;

    boolean DEBUG_FRAME_IO = false;

    int FRAME_FIELD_MAGIC = 0x12345678;

}
