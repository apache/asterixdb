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
import org.apache.asterix.om.base.APoint;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;

public class APointSerializerDeserializer implements ISerializerDeserializer<APoint> {

    private static final long serialVersionUID = 1L;

    public static final APointSerializerDeserializer INSTANCE = new APointSerializerDeserializer();

    private APointSerializerDeserializer() {
    }

    @Override
    public APoint deserialize(DataInput in) throws HyracksDataException {
        return read(in);
    }

    @Override
    public void serialize(APoint instance, DataOutput out) throws HyracksDataException {
        write(instance, out);
    }

    public static APoint read(DataInput in) throws HyracksDataException {
        return new APoint(DoubleSerializerDeserializer.read(in), DoubleSerializerDeserializer.read(in));
    }

    public static void write(APoint instance, DataOutput out) throws HyracksDataException {
        serialize(instance.getX(), instance.getY(), out);
    }

    public static void serialize(double x, double y, DataOutput out) throws HyracksDataException {
        DoubleSerializerDeserializer.write(x, out);
        DoubleSerializerDeserializer.write(y, out);
    }

    public final static int getCoordinateOffset(Coordinate coordinate) throws HyracksDataException {
        switch (coordinate) {
            case X:
                return 1;
            case Y:
                return 9;
            default:
                throw new HyracksDataException("Wrong coordinate");
        }
    }
}
