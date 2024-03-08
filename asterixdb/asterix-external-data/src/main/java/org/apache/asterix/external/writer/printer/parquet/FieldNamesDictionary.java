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
package org.apache.asterix.external.writer.printer.parquet;

import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class FieldNamesDictionary {
    private final IBinaryHashFunction fieldNameHashFunction;
    private final Int2ObjectMap<String> hashToFieldNameIndexMap;

    public FieldNamesDictionary() {
        fieldNameHashFunction =
                new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY).createBinaryHashFunction();
        hashToFieldNameIndexMap = new Int2ObjectOpenHashMap<>();
    }

    //TODO solve collision (they're so rare that I haven't seen any)
    public String getOrCreateFieldNameIndex(IValueReference pointable) throws HyracksDataException {

        int hash = getHash(pointable);
        if (!hashToFieldNameIndexMap.containsKey(hash)) {
            String fieldName = UTF8StringUtil.toString(pointable.getByteArray(), pointable.getStartOffset());
            hashToFieldNameIndexMap.put(hash, fieldName);
            return fieldName;
        }
        return hashToFieldNameIndexMap.get(hash);
    }

    private int getHash(IValueReference fieldName) throws HyracksDataException {
        byte[] object = fieldName.getByteArray();
        int start = fieldName.getStartOffset();
        int length = fieldName.getLength();
        return fieldNameHashFunction.hash(object, start, length);
    }
}
