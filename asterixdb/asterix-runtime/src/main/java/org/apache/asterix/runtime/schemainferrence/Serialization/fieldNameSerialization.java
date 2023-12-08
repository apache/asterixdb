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

package org.apache.asterix.runtime.schemainferrence.Serialization;

import java.io.IOException;

import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/*
Field name specific serialization for jackson JSON serialization
*/
public class fieldNameSerialization extends JsonSerializer<IValueReference> {

    @Override
    public void serialize(IValueReference iValueReference, JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider) throws IOException {
        ArrayBackedValueStorage fieldNameSize = new ArrayBackedValueStorage(1);
        fieldNameSize.set(iValueReference.getByteArray(), iValueReference.getStartOffset(), 1);
        String fieldName = new String(iValueReference.getByteArray(),
                iValueReference.getLength() - fieldNameSize.getByteArray()[0],
                iValueReference.getLength() - (iValueReference.getLength() - fieldNameSize.getByteArray()[0]));
        jsonGenerator.writeString(fieldName);
    }
}
