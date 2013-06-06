/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.api.comm;

import java.nio.ByteBuffer;

public interface IFrameTupleAccessor {
    public int getFieldCount();

    public int getFieldSlotsLength();

    public int getFieldEndOffset(int tupleIndex, int fIdx);

    public int getFieldStartOffset(int tupleIndex, int fIdx);

    public int getFieldLength(int tupleIndex, int fIdx);

    public int getTupleEndOffset(int tupleIndex);

    public int getTupleStartOffset(int tupleIndex);

    public int getTupleCount();

    public ByteBuffer getBuffer();

    public void reset(ByteBuffer buffer);
}