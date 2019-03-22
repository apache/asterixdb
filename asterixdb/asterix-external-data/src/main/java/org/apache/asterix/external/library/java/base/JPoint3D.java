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

import org.apache.asterix.dataflow.data.nontagged.serde.APoint3DSerializerDeserializer;
import org.apache.asterix.om.base.AMutablePoint3D;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class JPoint3D extends JObject {

    public JPoint3D(double x, double y, double z) {
        super(new AMutablePoint3D(x, y, z));
    }

    public void setValue(double x, double y, double z) {
        ((AMutablePoint3D) value).setValue(x, y, z);
    }

    public double getXValue() {
        return ((AMutablePoint3D) value).getX();
    }

    public double getYValue() {
        return ((AMutablePoint3D) value).getY();
    }

    public double getZValue() {
        return ((AMutablePoint3D) value).getZ();
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        serializeTypeTag(writeTypeTag, dataOutput, ATypeTag.POINT3D);
        APoint3DSerializerDeserializer.INSTANCE.serialize((AMutablePoint3D) value, dataOutput);
    }

    @Override
    public void reset() {
        ((AMutablePoint3D) value).setValue(0, 0, 0);
    }

    @Override
    public IAType getIAType() {
        return BuiltinType.APOINT3D;
    }
}
