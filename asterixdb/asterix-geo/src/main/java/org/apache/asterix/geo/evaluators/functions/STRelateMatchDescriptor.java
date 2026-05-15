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

import java.io.DataOutput;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.locationtech.jts.geom.IntersectionMatrix;

/**
 * ST_RelateMatch: pure DE-9IM string predicate — returns {@code true} when
 * the given 9-character intersection matrix string matches the given pattern
 * (which may contain {@code *}, {@code T}, {@code F}, or any of {@code 0/1/2}).
 * No geometry arguments — both inputs are strings. Implemented via JTS
 * {@code IntersectionMatrix.matches(String matrix, String pattern)}.
 */
public class STRelateMatchDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = STRelateMatchDescriptor::new;
    private static final long serialVersionUID = 1L;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_RELATE_MATCH;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new STRelateMatchEvaluator(args, ctx);
            }
        };
    }

    private class STRelateMatchEvaluator implements IScalarEvaluator {
        private final ArrayBackedValueStorage resultStorage;
        private final DataOutput out;
        private final IPointable inputArg0;
        private final IPointable inputArg1;
        private final IScalarEvaluator eval0;
        private final IScalarEvaluator eval1;
        private final UTF8StringPointable matrixPtr;
        private final UTF8StringPointable patternPtr;
        @SuppressWarnings("rawtypes")
        private final ISerializerDeserializer boolSerde;

        STRelateMatchEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg0 = new VoidPointable();
            inputArg1 = new VoidPointable();
            eval0 = args[0].createScalarEvaluator(ctx);
            eval1 = args[1].createScalarEvaluator(ctx);
            matrixPtr = new UTF8StringPointable();
            patternPtr = new UTF8StringPointable();
            boolSerde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();
            eval0.evaluate(tuple, inputArg0);
            eval1.evaluate(tuple, inputArg1);

            if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                return;
            }

            byte[] bytes0 = inputArg0.getByteArray();
            int offset0 = inputArg0.getStartOffset();
            int len0 = inputArg0.getLength();
            byte[] bytes1 = inputArg1.getByteArray();
            int offset1 = inputArg1.getStartOffset();
            int len1 = inputArg1.getLength();

            if (bytes0[offset0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            }
            if (bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            }

            matrixPtr.set(bytes0, offset0 + 1, len0 - 1);
            patternPtr.set(bytes1, offset1 + 1, len1 - 1);
            String matrix = matrixPtr.toString();
            String pattern = patternPtr.toString();

            boolean matches = IntersectionMatrix.matches(matrix, pattern);
            boolSerde.serialize(matches ? ABoolean.TRUE : ABoolean.FALSE, out);
            result.set(resultStorage);
        }
    }
}
