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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Manages the buffer space in the unit of frame.
 * This buffer manager is suitable for a structure that manages
 * the list of assigned frames on its own (e.g., SerializableHashTable class).
 */
public interface ISimpleFrameBufferManager {

    /**
     * Gets a frame from this buffer manager.
     */
    public ByteBuffer acquireFrame(int frameSize) throws HyracksDataException;

    /**
     * Releases a frame to this buffer manager.
     */
    public void releaseFrame(ByteBuffer frame);

}
