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

package org.apache.hyracks.storage.am.vector.utils;

import java.io.DataOutput;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Single authority for the VTree per-cluster directory ("metadata") tuple layout
 * {@code [key fields..., data_page_pointer : int]}, mirroring {@code VTreeDataTupleAccessor}.
 * <p>
 * A separator carries the <em>whole</em> ordering key of the last record on the data page it points
 * at, not just that key's leading distance. The key therefore occupies the entry's leading fields and
 * the pointer trails them, which is {@code BTreeNSMLeafFrame.split}'s convention: a directory search
 * compares a key tuple against a separator with the same comparator that orders data pages, and the
 * trailing pointer is simply not visited.
 * <p>
 * A separator holding only the distance would be a strict prefix of the key: a page whose records share
 * one distance would split into entries that cover a search key equally, and the router could not tell
 * which page the key belongs in.
 *
 * @see org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrame
 */
public final class VTreeMetadataTupleAccessor {

    private VTreeMetadataTupleAccessor() {
    }

    /**
     * Builds a directory entry from {@code key} and the page it points at. Key fields are copied
     * byte-for-byte, so the entry's leading fields are encoded exactly as the data page encodes them.
     */
    public static ITupleReference createMetadataTuple(ITupleReference key, int dataPageId) throws HyracksDataException {
        try {
            int numKeyFields = key.getFieldCount();
            ArrayTupleBuilder builder = new ArrayTupleBuilder(numKeyFields + 1);
            for (int i = 0; i < numKeyFields; i++) {
                builder.addField(key.getFieldData(i), key.getFieldStart(i), key.getFieldLength(i));
            }
            DataOutput out = builder.getDataOutput();
            out.writeInt(dataPageId);
            builder.addFieldEndOffset();
            ArrayTupleReference entry = new ArrayTupleReference();
            entry.reset(builder.getFieldEndOffsets(), builder.getByteArray());
            return entry;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Tuple schema {@code [key fields..., data_page_pointer : int]}. */
    public static ITypeTraits[] typeTraits(ITypeTraits[] keyTypeTraits) {
        ITypeTraits[] schema = new ITypeTraits[keyTypeTraits.length + 1];
        System.arraycopy(keyTypeTraits, 0, schema, 0, keyTypeTraits.length);
        schema[keyTypeTraits.length] = IntegerPointable.TYPE_TRAITS;
        return schema;
    }

    /**
     * Data-page pointer in the entry's last field (raw big-endian int, no type tag). Read from the
     * tuple's own field count so this holds for any key width.
     */
    public static int getDataPagePointer(ITupleReference tuple) {
        int f = tuple.getFieldCount() - 1;
        return IntegerPointable.getInteger(tuple.getFieldData(f), tuple.getFieldStart(f));
    }
}
