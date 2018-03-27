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
import java.nio.ByteBuffer;

import org.apache.asterix.om.base.AGeometry;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.esri.core.geometry.OperatorImportFromWkb;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.WkbImportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;

public class AGeometrySerializerDeserializer implements ISerializerDeserializer<AGeometry> {

    private static final long serialVersionUID = 1L;

    public static final AGeometrySerializerDeserializer INSTANCE = new AGeometrySerializerDeserializer();

    /**Use WGS 84 (EPSG:4326) as the default coordinate reference system*/
    public static final SpatialReference DEFAULT_CRS = SpatialReference.create(4326);

    private AGeometrySerializerDeserializer() {
    }

    @Override
    public AGeometry deserialize(DataInput in) throws HyracksDataException {
        try {
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            OGCGeometry geometry = OGCGeometry.createFromOGCStructure(
                    OperatorImportFromWkb.local().executeOGC(WkbImportFlags.wkbImportDefaults, buffer, null),
                    DEFAULT_CRS);
            return new AGeometry(geometry);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void serialize(AGeometry instance, DataOutput out) throws HyracksDataException {
        try {
            OGCGeometry geometry = instance.getGeometry();
            byte[] buffer = geometry.asBinary().array();
            // For efficiency, we store the size of the geometry in bytes in the first 32 bits
            // This allows AsterixDB to skip over this attribute if needed.
            out.writeInt(buffer.length);
            out.write(buffer);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
