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
package org.apache.asterix.geo.evaluators.functions;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import com.esri.core.geometry.ogc.OGCGeometry;

public abstract class AbstractSTGeometryDoubleNDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    abstract protected Object evaluateOGCGeometry(OGCGeometry geometry, double n) throws HyracksDataException;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {

                return new AbstractSTGeometryDoubleNEvaluator(args, ctx);
            }
        };
    }

    private class AbstractSTGeometryDoubleNEvaluator implements IScalarEvaluator {

        private ArrayBackedValueStorage resultStorage;
        private DataOutput out;
        private IPointable inputArg;
        private IScalarEvaluator eval0;
        private IPointable inputArg0;
        private IScalarEvaluator eval1;
        private ByteArrayAccessibleInputStream inStream;
        private DataInputStream dataIn;

        public AbstractSTGeometryDoubleNEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg = new VoidPointable();
            eval0 = args[0].createScalarEvaluator(ctx);
            inputArg0 = new VoidPointable();
            eval1 = args[1].createScalarEvaluator(ctx);
            inStream = new ByteArrayAccessibleInputStream(new byte[0], 0, 0);
            dataIn = new DataInputStream(inStream);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();

            eval0.evaluate(tuple, inputArg);
            byte[] data0 = inputArg.getByteArray();
            int offset = inputArg.getStartOffset();
            int len = inputArg.getLength();

            eval1.evaluate(tuple, inputArg0);
            byte[] data1 = inputArg0.getByteArray();
            int offset0 = inputArg0.getStartOffset();

            if (data0[offset] != ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data0[offset],
                        ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
            }

            inStream.setContent(data0, offset + 1, len - 1);
            OGCGeometry geometry = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn).getGeometry();
            Object finalResult;
            if (data1[offset0] == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
                finalResult =
                        evaluateOGCGeometry(geometry, ADoubleSerializerDeserializer.getDouble(data1, offset0 + 1));
            } else if (data1[offset0] == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                finalResult = evaluateOGCGeometry(geometry, AInt64SerializerDeserializer.getLong(data1, offset0 + 1));
            } else {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data1[offset0],
                        ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
            }

            try {
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ARECTANGLE)
                        .serialize(finalResult, out);
                result.set(resultStorage);
            } catch (IOException e) {
                throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e,
                        ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
            }
        }
    }
}
