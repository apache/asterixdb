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
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.base.AMutableInt32;
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
import org.apache.hyracks.util.string.UTF8StringUtil;

import com.esri.core.geometry.ogc.OGCGeometry;

public abstract class AbstractSTSingleGeometryDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    abstract protected Object evaluateOGCGeometry(OGCGeometry geometry) throws HyracksDataException;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            @SuppressWarnings("unchecked")
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractSTSingleGeometryEvaluator(args, ctx);
            }
        };
    }

    private class AbstractSTSingleGeometryEvaluator implements IScalarEvaluator {

        private final ArrayBackedValueStorage resultStorage;
        private final DataOutput out;
        private final IPointable argPtr0;
        private final IScalarEvaluator eval0;

        private final AMutableInt32 intRes;

        public AbstractSTSingleGeometryEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            argPtr0 = new VoidPointable();
            eval0 = args[0].createScalarEvaluator(ctx);
            intRes = new AMutableInt32(0);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();
            eval0.evaluate(tuple, argPtr0);

            try {
                byte[] bytes0 = argPtr0.getByteArray();
                int offset0 = argPtr0.getStartOffset();
                int len0 = argPtr0.getLength();

                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);
                if (tag != ATypeTag.GEOMETRY) {
                    throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                            ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                }

                DataInputStream dataIn0 = new DataInputStream(new ByteArrayInputStream(bytes0, offset0 + 1, len0 - 1));
                OGCGeometry geometry0 = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn0).getGeometry();

                Object finalResult = evaluateOGCGeometry(geometry0);
                if (finalResult == null) {
                    out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                } else if (finalResult instanceof Double) {
                    out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    out.writeDouble((double) finalResult);
                } else if (finalResult instanceof Boolean) {
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN)
                            .serialize((boolean) finalResult ? ABoolean.TRUE : ABoolean.FALSE, out);
                } else if (finalResult instanceof Integer) {
                    intRes.setValue((int) finalResult);
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32)
                            .serialize(intRes, out);
                } else if (finalResult instanceof String) {
                    out.write(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                    out.write(UTF8StringUtil.writeStringToBytes((String) finalResult));
                } else if (finalResult instanceof byte[]) {
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY)
                            .serialize(new ABinary((byte[]) finalResult), out);
                } else if (finalResult instanceof OGCGeometry) {
                    out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                    AGeometrySerializerDeserializer.INSTANCE.serialize(new AGeometry((OGCGeometry) finalResult), out);
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            result.set(resultStorage);
        }
    }
}
