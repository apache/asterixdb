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

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class IntervalSideTuple {
    // Tuple access
    int fieldId;
    ITupleCursor cursor;

    long start;
    long end;

    // Join details
    final IIntervalJoinUtil imjc;

    public IntervalSideTuple(IIntervalJoinUtil imjc, ITupleCursor cursor, int fieldId) {
        this.imjc = imjc;
        this.cursor = cursor;
        this.fieldId = fieldId;
    }

    public void loadTuple() {
        int offset = IntervalJoinUtil.getIntervalOffset(cursor.getAccessor(), cursor.getTupleId(), fieldId);
        start = AIntervalSerializerDeserializer.getIntervalStart(cursor.getAccessor().getBuffer().array(), offset);
        end = AIntervalSerializerDeserializer.getIntervalEnd(cursor.getAccessor().getBuffer().array(), offset);
    }

    public int getTupleIndex() {
        return cursor.getTupleId();
    }

    public ITupleCursor getCursor() {
        return cursor;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public boolean compareJoin(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToSaveInResult(cursor.getAccessor(), cursor.getTupleId(), ist.cursor.getAccessor(),
                ist.cursor.getTupleId());
    }

    public boolean removeFromMemory(IntervalSideTuple ist) {
        return imjc.checkToRemoveInMemory(cursor.getAccessor(), cursor.getTupleId(), ist.cursor.getAccessor(),
                ist.cursor.getTupleId());
    }

    public boolean checkForEarlyExit(IntervalSideTuple ist) {
        return imjc.checkForEarlyExit(cursor.getAccessor(), cursor.getTupleId(), ist.cursor.getAccessor(),
                ist.cursor.getTupleId());
    }
}
