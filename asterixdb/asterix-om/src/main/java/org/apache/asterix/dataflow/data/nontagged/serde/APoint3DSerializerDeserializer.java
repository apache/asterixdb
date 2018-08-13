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

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.om.base.APoint3D;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;

public class APoint3DSerializerDeserializer implements ISerializerDeserializer<APoint3D> {

    private static final long serialVersionUID = 1L;

    public static final APoint3DSerializerDeserializer INSTANCE = new APoint3DSerializerDeserializer();

    private APoint3DSerializerDeserializer() {
    }

    @Override
    public APoint3D deserialize(DataInput in) throws HyracksDataException {
        final double x = DoubleSerializerDeserializer.read(in);
        final double y = DoubleSerializerDeserializer.read(in);
        final double z = DoubleSerializerDeserializer.read(in);
        return new APoint3D(x, y, z);
    }

    @Override
    public void serialize(APoint3D instance, DataOutput out) throws HyracksDataException {
        DoubleSerializerDeserializer.write(instance.getX(), out);
        DoubleSerializerDeserializer.write(instance.getY(), out);
        DoubleSerializerDeserializer.write(instance.getZ(), out);
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
}
