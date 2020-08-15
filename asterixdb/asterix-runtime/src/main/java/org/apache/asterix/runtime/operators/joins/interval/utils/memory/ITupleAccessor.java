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

package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public interface ITupleAccessor extends IFrameTupleAccessor {
    int getTupleStartOffset();

    int getTupleEndOffset();

    int getTupleLength();

    int getAbsFieldStartOffset(int fieldId);

    int getFieldLength(int fieldId);

    @Override
    int getFieldCount();

    @Override
    int getFieldSlotsLength();

    int getFieldEndOffset(int fieldId);

    int getFieldStartOffset(int fieldId);

    void reset(TuplePointer tuplePointer);

    @Override
    void reset(ByteBuffer buffer);

    int getTupleId();

    void setTupleId(int tupleId);

    void getTuplePointer(TuplePointer tp);

    /**
     * Only reset the iterator.
     */
    void reset();

    boolean hasNext();

    void next();

    boolean exists();
}
