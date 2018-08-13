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
import org.apache.asterix.om.base.ARectangle;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ARectangleSerializerDeserializer implements ISerializerDeserializer<ARectangle> {

    private static final long serialVersionUID = 1L;

    public static final ARectangleSerializerDeserializer INSTANCE = new ARectangleSerializerDeserializer();

    private ARectangleSerializerDeserializer() {
    }

    @Override
    public ARectangle deserialize(DataInput in) throws HyracksDataException {
        return new ARectangle(APointSerializerDeserializer.read(in), APointSerializerDeserializer.read(in));
    }

    @Override
    public void serialize(ARectangle instance, DataOutput out) throws HyracksDataException {
        APointSerializerDeserializer.write(instance.getP1(), out);
        APointSerializerDeserializer.write(instance.getP2(), out);
    }

    public final static int getBottomLeftCoordinateOffset(Coordinate coordinate) throws HyracksDataException {
        switch (coordinate) {
            case X:
                return 1;
            case Y:
                return 9;
            default:
                throw new HyracksDataException("Wrong coordinate");
        }
    }

    public final static int getUpperRightCoordinateOffset(Coordinate coordinate) throws HyracksDataException {
        switch (coordinate) {
            case X:
                return 17;
            case Y:
                return 25;
            default:
                throw new HyracksDataException("Wrong coordinate");
        }
    }
}
