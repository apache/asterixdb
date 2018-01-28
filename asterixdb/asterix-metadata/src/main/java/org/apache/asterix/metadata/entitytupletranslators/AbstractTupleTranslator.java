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

package org.apache.asterix.metadata.entitytupletranslators;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslator;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;

/**
 * Contains common members shared across all concrete implementations of
 * IMetadataEntityTupleTranslator.
 */
public abstract class AbstractTupleTranslator<T> implements IMetadataEntityTupleTranslator<T> {
    private static final long serialVersionUID = 1L;
    protected AMutableString aString = new AMutableString("");
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt32> int32Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

    protected final transient IARecordBuilder recordBuilder;
    protected final transient ArrayBackedValueStorage fieldValue;
    protected final transient ArrayTupleBuilder tupleBuilder;
    protected final transient ArrayTupleReference tuple;

    public AbstractTupleTranslator(boolean getTuple, int fieldCount) {
        if (getTuple) {
            recordBuilder = new RecordBuilder();
            fieldValue = new ArrayBackedValueStorage();
            tupleBuilder = new ArrayTupleBuilder(fieldCount);
            tuple = new ArrayTupleReference();
        } else {
            recordBuilder = null;
            fieldValue = null;
            tupleBuilder = null;
            tuple = null;
        }
    }
}
