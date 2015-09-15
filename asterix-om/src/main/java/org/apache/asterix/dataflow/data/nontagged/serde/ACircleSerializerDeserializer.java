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
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.AMutableCircle;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ACircleSerializerDeserializer implements ISerializerDeserializer<ACircle> {

    private static final long serialVersionUID = 1L;

    public static final ACircleSerializerDeserializer INSTANCE = new ACircleSerializerDeserializer();

    @SuppressWarnings("unchecked")
    private final static ISerializerDeserializer<ACircle> circleSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ACIRCLE);
    private final static AMutableCircle aCircle = new AMutableCircle(null, 0);
    private final static AMutablePoint aPoint = new AMutablePoint(0, 0);

    private ACircleSerializerDeserializer() {
    }

    @Override
    public ACircle deserialize(DataInput in) throws HyracksDataException {
        try {
            APoint center = APointSerializerDeserializer.INSTANCE.deserialize(in);
            double radius = ADoubleSerializerDeserializer.INSTANCE.deserialize(in).getDoubleValue();
            return new ACircle(center, radius);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ACircle instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeDouble(instance.getP().getX());
            out.writeDouble(instance.getP().getY());
            out.writeDouble(instance.getRadius());
        } catch (IOException e) {
            throw new HyracksDataException();
        }
    }

    public final static int getCenterPointCoordinateOffset(Coordinate coordinate) throws HyracksDataException {
        switch (coordinate) {
            case X:
                return 1;
            case Y:
                return 9;
            default:
                throw new HyracksDataException("Wrong coordinate");
        }
    }

    public final static int getRadiusOffset() throws HyracksDataException {
        return 17;
    }

    public static void parse(String circle, DataOutput out) throws HyracksDataException {
        try {
            String[] parts = circle.split(" ");
            aPoint.setValue(Double.parseDouble(parts[0].split(",")[0]), Double.parseDouble(parts[0].split(",")[1]));
            aCircle.setValue(aPoint, Double.parseDouble(parts[1].substring(0, parts[1].length())));
            circleSerde.serialize(aCircle, out);
        } catch (HyracksDataException e) {
            throw new HyracksDataException(circle + " can not be an instance of circle");
        }
    }
}
