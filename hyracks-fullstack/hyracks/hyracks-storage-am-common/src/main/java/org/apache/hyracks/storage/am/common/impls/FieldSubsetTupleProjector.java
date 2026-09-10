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
package org.apache.hyracks.storage.am.common.impls;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.projection.ITupleProjector;

/** Emits a caller-chosen subset of a tuple's fields, in the order listed. */
class FieldSubsetTupleProjector implements ITupleProjector {

    /** Stored-tuple fields to emit, in output order. */
    private final int[] projectedFields;

    FieldSubsetTupleProjector(int[] projectedFields) {
        this.projectedFields = projectedFields;
    }

    /** Writes the projected fields to {@code dos} and {@code tb}; the return value is the input, not the projection. */
    @Override
    public ITupleReference project(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb) throws IOException {
        int totalFields = tuple.getFieldCount();
        for (int field : projectedFields) {
            if (field >= totalFields) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "The projection needs field " + field + " of a tuple that only has " + totalFields + " fields");
            }
            dos.write(tuple.getFieldData(field), tuple.getFieldStart(field), tuple.getFieldLength(field));
            tb.addFieldEndOffset();
        }
        return tuple;
    }
}
