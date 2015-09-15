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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint3D;
import org.apache.asterix.om.base.APoint3D;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class APoint3DSerializerDeserializer implements ISerializerDeserializer<APoint3D> {

    private static final long serialVersionUID = 1L;

    public static final APoint3DSerializerDeserializer INSTANCE = new APoint3DSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private final static ISerializerDeserializer<APoint3D> point3DSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.APOINT3D);
    private final static AMutablePoint3D aPoint3D = new AMutablePoint3D(0, 0, 0);

    private APoint3DSerializerDeserializer() {
    }

    @Override
    public APoint3D deserialize(DataInput in) throws HyracksDataException {
        try {
            return new APoint3D(in.readDouble(), in.readDouble(), in.readDouble());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(APoint3D instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeDouble(instance.getX());
            out.writeDouble(instance.getY());
            out.writeDouble(instance.getZ());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void stringToPoint3d(String instance, DataOutput out) throws HyracksDataException {
        try {
            int firstCommaIndex = instance.indexOf(',');
            int secondCommaIndex = instance.indexOf(',', firstCommaIndex + 1);
            out.writeByte(ATypeTag.POINT3D.serialize());
            out.writeDouble(Double.parseDouble(instance.substring(0, firstCommaIndex)));
            out.writeDouble(Double.parseDouble(instance.substring(firstCommaIndex + 1, secondCommaIndex)));
            out.writeDouble(Double.parseDouble(instance.substring(secondCommaIndex + 1, instance.length())));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public final static int getCoordinateOffset(Coordinate coordinate) throws HyracksDataException {
        switch (coordinate) {
            case X:
                return 1;
            case Y:
                return 9;
            case Z:
                return 17;
            default:
                throw new HyracksDataException("Wrong coordinate");
        }
    }

    public static void parse(String point3d, DataOutput out) throws HyracksDataException {
        try {
            int firstCommaIndex = point3d.indexOf(',');
            int secondCommaIndex = point3d.indexOf(',', firstCommaIndex + 1);
            aPoint3D.setValue(Double.parseDouble(point3d.substring(0, firstCommaIndex)),
                    Double.parseDouble(point3d.substring(firstCommaIndex + 1, secondCommaIndex)),
                    Double.parseDouble(point3d.substring(secondCommaIndex + 1, point3d.length())));
            point3DSerde.serialize(aPoint3D, out);
        } catch (HyracksDataException e) {
            throw new HyracksDataException(point3d + " can not be an instance of point3d");
        }
    }
}
