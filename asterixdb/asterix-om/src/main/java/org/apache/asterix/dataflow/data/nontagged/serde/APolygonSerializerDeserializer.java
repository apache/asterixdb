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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.APolygon;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;

public class APolygonSerializerDeserializer implements ISerializerDeserializer<APolygon> {

    private static final long serialVersionUID = 1L;

    public static final APolygonSerializerDeserializer INSTANCE = new APolygonSerializerDeserializer();

    private APolygonSerializerDeserializer() {
    }

    @Override
    public APolygon deserialize(DataInput in) throws HyracksDataException {
        short numberOfPoints = ShortSerializerDeserializer.read(in);
        APoint[] points = new APoint[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            points[i] = APointSerializerDeserializer.read(in);
        }
        return new APolygon(points);
    }

    @Override
    public void serialize(APolygon instance, DataOutput out) throws HyracksDataException {
        short n = (short) instance.getNumberOfPoints();
        ShortSerializerDeserializer.write(n, out);
        for (int i = 0; i < n; i++) {
            APointSerializerDeserializer.write(instance.getPoints()[i], out);
        }
    }

    public static int getNumberOfPointsOffset() throws HyracksDataException {
        return 1;
    }

    public static int getCoordinateOffset(int pointId, Coordinate coordinate) throws HyracksDataException {
        switch (coordinate) {
            case X:
                return 3 + (pointId * 16);
            case Y:
                return 11 + (pointId * 16);
            default:
                throw HyracksDataException.create(ErrorCode.POLYGON_INVALID_COORDINATE);
        }
    }

    public static void parse(String polygon, DataOutput out) throws HyracksDataException {
        try {
            String[] points = polygon.split(" ");
            if (points.length < 3) {
                throw HyracksDataException.create(ErrorCode.POLYGON_3_POINTS);
            }
            out.writeByte(ATypeTag.POLYGON.serialize());
            out.writeShort(points.length);
            for (int i = 0; i < points.length; i++) {
                final String[] split = points[i].split(",");
                APointSerializerDeserializer.serialize(Double.parseDouble(split[0]), Double.parseDouble(split[1]), out);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(ErrorCode.POLYGON_INVALID, e, polygon);
        }
    }
}
