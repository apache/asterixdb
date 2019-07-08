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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.types.ATypeTag;
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
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import com.esri.core.geometry.ogc.OGCGeometry;

public abstract class AbstractSTGeometryNDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    abstract protected OGCGeometry evaluateOGCGeometry(OGCGeometry geometry, int n) throws HyracksDataException;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {

                return new AbstractSTGeometryNEvaluator(args, ctx);
            }
        };
    }

    private class AbstractSTGeometryNEvaluator implements IScalarEvaluator {

        private ArrayBackedValueStorage resultStorage;
        private DataOutput out;
        private IPointable inputArg;
        private IScalarEvaluator eval;
        private IPointable inputArg0;
        private IScalarEvaluator eval0;

        public AbstractSTGeometryNEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg = new VoidPointable();
            eval = args[0].createScalarEvaluator(ctx);
            inputArg0 = new VoidPointable();
            eval0 = args[1].createScalarEvaluator(ctx);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            eval.evaluate(tuple, inputArg);
            byte[] data = inputArg.getByteArray();
            int offset = inputArg.getStartOffset();
            int len = inputArg.getLength();

            eval0.evaluate(tuple, inputArg0);
            byte[] data0 = inputArg0.getByteArray();
            int offset0 = inputArg0.getStartOffset();

            if (data[offset] != ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data[offset],
                        ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
            }
            if (data0[offset0] != ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data0[offset0],
                        ATypeTag.SERIALIZED_INT64_TYPE_TAG);
            }

            ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset + 1, len - 1);
            DataInputStream dataIn = new DataInputStream(inStream);
            OGCGeometry geometry = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn).getGeometry();
            int n = (int) AInt64SerializerDeserializer.getLong(data0, offset0 + 1);

            OGCGeometry geometryN = evaluateOGCGeometry(geometry, n);
            try {
                out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                AGeometrySerializerDeserializer.INSTANCE.serialize(new AGeometry(geometryN), out);
                result.set(resultStorage);
            } catch (IOException e) {
                throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e,
                        ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
            }
        }
    }
}
