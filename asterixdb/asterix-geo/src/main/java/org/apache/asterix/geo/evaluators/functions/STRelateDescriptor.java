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
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import com.esri.core.geometry.ogc.OGCGeometry;

public class STRelateDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STRelateDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_RELATE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new STRelateEvaluator(args, ctx);
            }
        };
    }

    private class STRelateEvaluator implements IScalarEvaluator {
        private ArrayBackedValueStorage resultStorage;
        private DataOutput out;
        private IPointable inputArg;
        private IScalarEvaluator eval;
        private IPointable inputArg0;
        private IScalarEvaluator eval0;
        private final IPointable inputArg1;
        private final IScalarEvaluator eval1;

        public STRelateEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg = new VoidPointable();
            eval = args[2].createScalarEvaluator(ctx);
            inputArg0 = new VoidPointable();
            eval0 = args[0].createScalarEvaluator(ctx);
            inputArg1 = new VoidPointable();
            eval1 = args[1].createScalarEvaluator(ctx);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            eval.evaluate(tuple, inputArg);
            byte[] bytes = inputArg.getByteArray();
            int offset = inputArg.getStartOffset();
            int len = inputArg.getLength();

            eval0.evaluate(tuple, inputArg0);
            byte[] bytes0 = inputArg0.getByteArray();
            int offset0 = inputArg0.getStartOffset();
            int len0 = inputArg0.getLength();

            eval1.evaluate(tuple, inputArg1);
            byte[] bytes1 = inputArg1.getByteArray();
            int offset1 = inputArg1.getStartOffset();
            int len1 = inputArg1.getLength();

            if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes[offset],
                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            }
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

            ByteArrayInputStream inStream = new ByteArrayInputStream(bytes, offset + 1, len - 1);
            DataInputStream dataIn = new DataInputStream(inStream);
            String matrix = AStringSerializerDeserializer.INSTANCE.deserialize(dataIn).getStringValue();
            DataInputStream dataIn0 = new DataInputStream(new ByteArrayInputStream(bytes0, offset0 + 1, len0 - 1));
            OGCGeometry geometry0 = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn0).getGeometry();
            DataInputStream dataIn1 = new DataInputStream(new ByteArrayInputStream(bytes1, offset1 + 1, len1 - 1));
            OGCGeometry geometry1 = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn1).getGeometry();
            try {
                boolean val = geometry0.relate(geometry1, matrix);
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN)
                        .serialize(val ? ABoolean.TRUE : ABoolean.FALSE, out);
            } catch (IOException e) {
                throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e,
                        ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
            }
            result.set(resultStorage);
        }
    }
}
