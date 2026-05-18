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

package org.apache.hyracks.storage.am.vector.tuples;

import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;

/**
 * Helpers for working with VTree tuples (copying and primary-key extraction).
 * Input tuple field layout: vector first, primary key last, include fields in between.
 */
public class VTreeTupleUtils {
    /**
     * Returns the bytes of the primary key field (the last field of the tuple).
     * Only single-field primary keys are currently supported.
     *
     * @param tuple input tuple; must have at least 2 fields
     * @return primary-key bytes, or {@code null} if the tuple is null/too short or the field is empty
     */
    public static byte[] extractPrimaryKeyFromTuple(ITupleReference tuple) {
        if (tuple == null || tuple.getFieldCount() < 2) {
            return null;
        }

        int pkFieldIndex = tuple.getFieldCount() - 1;

        byte[] data = tuple.getFieldData(pkFieldIndex);
        if (data == null) {
            return null;
        }

        int offset = tuple.getFieldStart(pkFieldIndex);
        int length = tuple.getFieldLength(pkFieldIndex);

        if (length <= 0) {
            return null;
        }

        return Arrays.copyOfRange(data, offset, offset + length);
    }

    /**
     * Decode the vector stored at {@code fieldIndex} of the tuple using the supplied accessor factory.
     *
     * @param tuple      input tuple; must have at least {@code fieldIndex + 1} fields
     * @param fieldIndex zero-based index of the vector field
     * @param factory    accessor factory used to decode the field bytes
     * @return decoded vector, or {@code null} if the tuple or factory is unusable
     * @throws HyracksDataException if the underlying accessor fails to decode
     */
    public static double[] extractVectorFromTuple(ITupleReference tuple, int fieldIndex,
            IVTreeBinaryAccessorFactory factory) throws HyracksDataException {
        if (tuple == null || tuple.getFieldCount() <= fieldIndex || factory == null) {
            return null;
        }
        IVTreeBinaryAccessor accessor = factory.createAccessor();
        accessor.reset(tuple.getFieldData(fieldIndex), tuple.getFieldStart(fieldIndex),
                tuple.getFieldLength(fieldIndex));
        return accessor.getVector();
    }
}
