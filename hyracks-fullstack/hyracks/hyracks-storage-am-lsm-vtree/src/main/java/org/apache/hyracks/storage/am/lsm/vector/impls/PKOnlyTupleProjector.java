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
package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.projection.ITupleProjector;

/**
 * Tuple projector that extracts only primary key fields from vector index search results.
 *
 * Vector index tuples contain: <distance, cosine, embedding, pk1, pk2, ...>
 * This projector skips the secondary key fields (distance, cosine, embedding) and
 * writes only the primary key fields to the output.
 *
 * This optimization avoids transferring large embedding vectors (4KB-16KB) when only
 * primary keys are needed for subsequent primary index lookup.
 */
public class PKOnlyTupleProjector implements ITupleProjector {

    private final int numSecondaryKeys; // Number of fields to skip
    private final int numPrimaryKeys; // Number of PK fields to write

    public PKOnlyTupleProjector(int numSecondaryKeys, int numPrimaryKeys) {
        this.numSecondaryKeys = numSecondaryKeys;
        this.numPrimaryKeys = numPrimaryKeys;
    }

    /**
     * Writes only the primary-key fields into {@code dos}/{@code tb}; the meaningful projected output is
     * that builder, NOT the return value. The original (unprojected) tuple is returned merely as a
     * non-null sentinel — callers must consume {@code tb}, not the returned reference.
     */
    @Override
    public ITupleReference project(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb) throws IOException {
        int totalFields = tuple.getFieldCount();
        int startField = numSecondaryKeys;
        int endField = numSecondaryKeys + numPrimaryKeys;

        if (endField > totalFields) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "The primary-key projection needs fields ["
                    + startField + ", " + endField + ") of a tuple that only has " + totalFields + " fields");
        }

        for (int i = startField; i < endField; i++) {
            dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            tb.addFieldEndOffset();
        }

        return tuple;
    }
}
