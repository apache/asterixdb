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
package org.apache.asterix.external.parser.jackson;

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.external.parser.AbstractNestedDataParser;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * A state class that helps parsers of class {@link AbstractNestedDataParser} to maintain
 * its temporary buffers.
 */
public class ParserContext {
    private static final int SERIALIZED_FIELDNAME_MAP_MAX_SIZE = 128;

    private final ObjectPool<IARecordBuilder, ATypeTag> objectBuilderPool;
    private final ObjectPool<IAsterixListBuilder, ATypeTag> arrayBuilderPool;

    /**
     * Parsing nested structure using temporary buffers is expensive.
     * Example:
     * {"a":{"b":{"c":{"d":5}}}}
     *
     * Scalar value 5 is written 4 times in tempBuffer("d") then tempBuffer("c") ... tempBuffer("a")
     */
    private final ObjectPool<IMutableValueStorage, ATypeTag> tempBufferPool;
    private final ObjectPool<BitSet, Void> nullBitmapPool;
    private final Map<String, IMutableValueStorage> serializedFieldNames;
    private final ISerializerDeserializer<AString> stringSerDe;
    private final AMutableString aString;

    @SuppressWarnings("unchecked")
    public ParserContext() {
        objectBuilderPool = new ObjectPool<>(new RecordBuilderFactory());
        arrayBuilderPool = new ObjectPool<>(new ListBuilderFactory(), ATypeTag.ARRAY);
        tempBufferPool = new ObjectPool<>(new AbvsBuilderFactory());
        nullBitmapPool = new ObjectPool<>();
        serializedFieldNames = new LRUMap<>(SERIALIZED_FIELDNAME_MAP_MAX_SIZE);
        stringSerDe = SerializerDeserializerProvider.INSTANCE.getAStringSerializerDeserializer();
        aString = new AMutableString("");
    }

    public IMutableValueStorage enterObject() {
        return tempBufferPool.getInstance();
    }

    public BitSet getNullBitmap(int size) {
        if (size < 1) {
            return null;
        }

        BitSet nullBitMap = nullBitmapPool.getInstance();
        if (nullBitMap == null) {
            nullBitMap = new BitSet(size);
        }
        return nullBitMap;
    }

    public IARecordBuilder getObjectBuilder(ARecordType recordType) {
        IARecordBuilder builder = objectBuilderPool.getInstance();
        builder.reset(recordType);
        return builder;
    }

    /**
     * Experimental.
     * Check if too many serialization for the same field names can be expensive or not.
     *
     * @param fieldName
     * @return
     * @throws HyracksDataException
     */
    public IMutableValueStorage getSerializedFieldName(String fieldName) throws IOException {
        IMutableValueStorage serializedFieldName = serializedFieldNames.get(fieldName);
        if (serializedFieldName == null) {
            serializedFieldName = new ArrayBackedValueStorage();
            serializedFieldName.reset();
            aString.setValue(fieldName);
            stringSerDe.serialize(aString, serializedFieldName.getDataOutput());
            serializedFieldNames.put(fieldName, serializedFieldName);
        }
        return serializedFieldName;
    }

    public void exitObject(IMutableValueStorage tempBuffer, BitSet nullBitmap, IARecordBuilder builder) {
        tempBufferPool.recycle(tempBuffer);
        objectBuilderPool.recycle(builder);
        if (nullBitmap != null) {
            nullBitmap.clear();
            nullBitmapPool.recycle(nullBitmap);
        }
    }

    public IMutableValueStorage enterCollection() {
        return tempBufferPool.getInstance();
    }

    public IAsterixListBuilder getCollectionBuilder(AbstractCollectionType collectionType) {
        IAsterixListBuilder builder = arrayBuilderPool.getInstance();
        builder.reset(collectionType);
        return builder;
    }

    public void exitCollection(IMutableValueStorage tempBuffer, IAsterixListBuilder builder) {
        tempBufferPool.recycle(tempBuffer);
        arrayBuilderPool.recycle(builder);
    }

}
