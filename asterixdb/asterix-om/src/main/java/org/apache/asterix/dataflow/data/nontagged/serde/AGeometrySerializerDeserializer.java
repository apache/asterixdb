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
import java.util.Arrays;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.GeoFunctionUtils;
import org.apache.asterix.om.base.AGeometry;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

public class AGeometrySerializerDeserializer implements ISerializerDeserializer<AGeometry> {

    private static final long serialVersionUID = 1L;

    public static final AGeometrySerializerDeserializer INSTANCE = new AGeometrySerializerDeserializer();

    private AGeometrySerializerDeserializer() {
    }

    @Override
    public AGeometry deserialize(DataInput in) throws HyracksDataException {
        WKBReader wkbReader = new WKBReader();
        try {
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            Geometry geometry = wkbReader.read(bytes);
            return new AGeometry(geometry);
        } catch (IOException | ParseException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void serialize(AGeometry instance, DataOutput out) throws HyracksDataException {
        try {
            Geometry geometry = instance.getGeometry();
            WKBWriter wkbWriter = new WKBWriter(GeoFunctionUtils.getCoordinateDimension(geometry),
                    GeoFunctionUtils.LITTLE_ENDIAN_BYTEORDER);
            byte[] buffer = wkbWriter.write(geometry);
            // For efficiency, we store the size of the geometry in bytes in the first 32 bits
            // This allows AsterixDB to skip over this attribute if needed.
            out.writeInt(buffer.length);
            out.write(buffer);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void serialize(Geometry geometry, DataOutput out) throws HyracksDataException {
        try {
            WKBWriter wkbWriter = new WKBWriter(GeoFunctionUtils.getCoordinateDimension(geometry),
                    GeoFunctionUtils.LITTLE_ENDIAN_BYTEORDER);
            byte[] buffer = wkbWriter.write(geometry);
            // For efficiency, we store the size of the geometry in bytes in the first 32 bits
            // This allows AsterixDB to skip over this attribute if needed.
            out.writeInt(buffer.length);
            out.write(buffer);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static int getAGeometrySizeOffset() {
        return 0;
    }

    public static AGeometry getAGeometryObject(byte[] bytes, int startOffset) throws HyracksDataException {
        // Size of the AGeometry object is stored in bytes in the first 32 bits
        // See serialize method
        WKBReader wkbReader = new WKBReader();
        int size = AInt32SerializerDeserializer.getInt(bytes, startOffset);

        if (bytes.length < startOffset + size + 4)
            // TODO(mmahin): this error code takes 5 parameters, and this is passing none, so I suspect this isn't right
            throw RuntimeDataException.create(ErrorCode.VALUE_OUT_OF_RANGE);
        try {
            // Skip the size of the geometry in first 4 bytes
            byte[] bytes1 = Arrays.copyOfRange(bytes, startOffset + 4, startOffset + size + 4);
            Geometry geometry = wkbReader.read(bytes1);
            return new AGeometry(geometry);
        } catch (ParseException e) {
            throw HyracksDataException.create(e);
        }
    }
}
