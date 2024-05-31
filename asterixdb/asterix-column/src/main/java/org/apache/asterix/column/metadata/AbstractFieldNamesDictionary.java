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

package org.apache.asterix.column.metadata;

import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public abstract class AbstractFieldNamesDictionary implements IFieldNamesDictionary {
    /**
     * Dummy field name used to add a column when encountering empty object
     */
    public static final IValueReference DUMMY_FIELD_NAME;
    public static final int DUMMY_FIELD_NAME_INDEX = -1;

    //For declared fields
    private final AMutableString mutableString;
    private final AStringSerializerDeserializer stringSerDer;

    static {
        VoidPointable dummy = new VoidPointable();
        dummy.set(new byte[0], 0, 0);
        DUMMY_FIELD_NAME = dummy;
    }

    AbstractFieldNamesDictionary() {
        mutableString = new AMutableString("");
        stringSerDer = new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
    }

    static ArrayBackedValueStorage creatFieldName(IValueReference fieldName) throws HyracksDataException {
        ArrayBackedValueStorage copy = new ArrayBackedValueStorage(fieldName.getLength());
        copy.append(fieldName);
        return copy;
    }

    protected ArrayBackedValueStorage creatFieldName(String fieldName) throws HyracksDataException {
        ArrayBackedValueStorage serializedFieldName = new ArrayBackedValueStorage();
        serializeFieldName(fieldName, serializedFieldName);
        return serializedFieldName;
    }

    protected void serializeFieldName(String fieldName, ArrayBackedValueStorage storage) throws HyracksDataException {
        mutableString.setValue(fieldName);
        stringSerDer.serialize(mutableString, storage.getDataOutput());
    }
}
