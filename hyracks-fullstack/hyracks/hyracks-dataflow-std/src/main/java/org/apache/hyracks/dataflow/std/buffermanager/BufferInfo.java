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

import org.apache.hyracks.dataflow.std.structures.IResetable;

public class BufferInfo implements IResetable<BufferInfo> {
    private ByteBuffer buffer;
    private int startOffset;
    private int length;

    public BufferInfo(ByteBuffer buffer, int startOffset, int length) {
        reset(buffer, startOffset, length);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getLength() {
        return length;
    }

    @Override
    public void reset(BufferInfo other) {
        this.buffer = other.buffer;
        this.startOffset = other.startOffset;
        this.length = other.length;
    }

    public void reset(ByteBuffer buffer, int startOffset, int length) {
        this.buffer = buffer;
        this.startOffset = startOffset;
        this.length = length;
    }
}
