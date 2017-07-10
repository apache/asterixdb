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
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.APoint;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;

public class ACircleSerializerDeserializer implements ISerializerDeserializer<ACircle> {

    private static final long serialVersionUID = 1L;

    public static final ACircleSerializerDeserializer INSTANCE = new ACircleSerializerDeserializer();

    private ACircleSerializerDeserializer() {
    }

    @Override
    public ACircle deserialize(DataInput in) throws HyracksDataException {
        APoint center = APointSerializerDeserializer.read(in);
        double radius = DoubleSerializerDeserializer.read(in);
        return new ACircle(center, radius);
    }

    @Override
    public void serialize(ACircle instance, DataOutput out) throws HyracksDataException {
        APointSerializerDeserializer.write(instance.getP(), out);
        DoubleSerializerDeserializer.write(instance.getRadius(), out);
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
}
