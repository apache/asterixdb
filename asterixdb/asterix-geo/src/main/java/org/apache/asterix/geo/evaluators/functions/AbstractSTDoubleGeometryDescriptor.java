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
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
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

public abstract class AbstractSTDoubleGeometryDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    abstract protected Object evaluateOGCGeometry(OGCGeometry geometry0, OGCGeometry geometry1)
            throws HyracksDataException;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractSTDoubleGeometryEvaluator(args, ctx);
            }
        };
    }

    private class AbstractSTDoubleGeometryEvaluator implements IScalarEvaluator {

        private final ArrayBackedValueStorage resultStorage;
        private final DataOutput out;
        private final IPointable argPtr0;
        private final IPointable argPtr1;
        private final IScalarEvaluator eval0;
        private final IScalarEvaluator eval1;

        public AbstractSTDoubleGeometryEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            argPtr0 = new VoidPointable();
            argPtr1 = new VoidPointable();
            eval0 = args[0].createScalarEvaluator(ctx);
            eval1 = args[1].createScalarEvaluator(ctx);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();
            eval0.evaluate(tuple, argPtr0);
            eval1.evaluate(tuple, argPtr1);

            try {
                byte[] bytes0 = argPtr0.getByteArray();
                int offset0 = argPtr0.getStartOffset();
                int len0 = argPtr0.getLength();
                byte[] bytes1 = argPtr1.getByteArray();
                int offset1 = argPtr1.getStartOffset();
                int len1 = argPtr1.getLength();

                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);
                if (tag != ATypeTag.GEOMETRY) {
                    throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                            ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                }
                tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);
                if (tag != ATypeTag.GEOMETRY) {
                    throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes1[offset1],
                            ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                }

                DataInputStream dataIn0 = new DataInputStream(new ByteArrayInputStream(bytes0, offset0 + 1, len0 - 1));
                OGCGeometry geometry0 = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn0).getGeometry();
                DataInputStream dataIn1 = new DataInputStream(new ByteArrayInputStream(bytes1, offset1 + 1, len1 - 1));
                OGCGeometry geometry1 = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn1).getGeometry();
                Object finalResult = evaluateOGCGeometry(geometry0, geometry1);
                if (finalResult instanceof OGCGeometry) {
                    out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                    AGeometrySerializerDeserializer.INSTANCE.serialize(new AGeometry((OGCGeometry) finalResult), out);
                } else if (finalResult instanceof Boolean) {
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN)
                            .serialize((boolean) finalResult ? ABoolean.TRUE : ABoolean.FALSE, out);
                } else if (finalResult instanceof Double) {
                    out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    out.writeDouble((double) finalResult);
                }

            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            result.set(resultStorage);
        }
    }
}
