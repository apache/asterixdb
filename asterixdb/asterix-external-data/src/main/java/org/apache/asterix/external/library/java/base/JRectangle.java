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

import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class JRectangle extends JObject {

    public JRectangle(JPoint p1, JPoint p2) {
        super(new AMutableRectangle((APoint) p1.getIAObject(), (APoint) p2.getIAObject()));
    }

    public void setValue(JPoint p1, JPoint p2) {
        ((AMutableRectangle) value).setValue((APoint) p1.getValue(), (APoint) p2.getValue());
    }

    public void setValue(APoint p1, APoint p2) {
        ((AMutableRectangle) value).setValue(p1, p2);
    }

    public ARectangle getValue() {
        return (AMutableRectangle) value;
    }

    public IAType getIAType() {
        return BuiltinType.ARECTANGLE;
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        serializeTypeTag(writeTypeTag, dataOutput, ATypeTag.RECTANGLE);
        ARectangleSerializerDeserializer.INSTANCE.serialize((ARectangle) value, dataOutput);
    }

    @Override
    public void reset() {
        ((AMutableRectangle) value).setValue(null, null);
    }
}
