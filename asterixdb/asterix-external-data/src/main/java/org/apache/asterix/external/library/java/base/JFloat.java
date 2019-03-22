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
package org.apache.asterix.external.library.java.base;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class JFloat extends JObject {

    public JFloat(float v) {
        super(new AMutableFloat(v));
    }

    public void setValue(float v) {
        ((AMutableFloat) value).setValue(v);
    }

    public float getValue() {
        return ((AMutableFloat) value).getFloatValue();
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        serializeTypeTag(writeTypeTag, dataOutput, ATypeTag.FLOAT);
        AFloatSerializerDeserializer.INSTANCE.serialize((AFloat) value, dataOutput);
    }

    @Override
    public void reset() {
        ((AMutableFloat) value).setValue(0);
    }

    @Override
    public IAType getIAType() {
        return BuiltinType.AFLOAT;
    }
}
