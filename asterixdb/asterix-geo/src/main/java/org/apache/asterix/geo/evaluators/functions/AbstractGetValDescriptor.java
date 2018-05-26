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
package org.apache.asterix.geo.evaluators.functions;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;

public abstract class AbstractGetValDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    public double getVal(byte[] bytes, int offset) throws TypeMismatchException {
        if (bytes[offset] == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
            return ADoubleSerializerDeserializer.getDouble(bytes, offset + 1);
        } else if (bytes[offset] == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
            return AInt64SerializerDeserializer.getLong(bytes, offset + 1);
        } else {
            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes[offset],
                    ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }
    }

}
