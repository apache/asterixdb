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
package org.apache.asterix.runtime.evaluators.functions.vector;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * {@code normalize_vector(v)} returns the unit-length vector in the same direction as {@code v}.
 * <p>
 * Missing and null propagate. An empty list, a non-list, a non-numeric element, a non-finite component,
 * or a zero vector has no direction to preserve, so the result is null (with a warning), matching
 * {@code cosine_similarity}'s zero-norm → NaN → null path rather than {@code isvector}'s boolean.
 */
@MissingNullInOutFunction
public class NormalizeVectorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = NormalizeVectorDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NORMALIZE_VECTOR;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new NormalizeVectorEvaluator(args, ctx, getIdentifier(), sourceLoc);
            }
        };
    }

    private static final class NormalizeVectorEvaluator implements IScalarEvaluator {
        private static final AOrderedListType DOUBLE_LIST = new AOrderedListType(BuiltinType.ADOUBLE, null);

        private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
        private final DataOutput out = resultStorage.getDataOutput();
        private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
        private final IPointable valuePtr = new VoidPointable();
        private final IScalarEvaluator valueEval;
        private final VectorListDecoder decoder = new VectorListDecoder();
        private final ListAccessor listAccessor = new ListAccessor();
        private final OrderedListBuilder listBuilder = new OrderedListBuilder();
        private final AMutableDouble aDouble = new AMutableDouble(0);
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ADouble> doubleSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
        private final IEvaluatorContext ctx;
        private final FunctionIdentifier funcId;
        private final SourceLocation sourceLoc;
        private double[] vector = new double[0];

        private NormalizeVectorEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx,
                FunctionIdentifier funcId, SourceLocation sourceLoc) throws HyracksDataException {
            this.valueEval = args[0].createScalarEvaluator(ctx);
            this.ctx = ctx;
            this.funcId = funcId;
            this.sourceLoc = sourceLoc;
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            valueEval.evaluate(tuple, valuePtr);
            if (PointableHelper.checkAndSetMissingOrNull(result, valuePtr)) {
                return;
            }
            resultStorage.reset();
            try {
                if (!decoder.checkListType(valuePtr)) {
                    ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcId,
                            valuePtr.getByteArray()[valuePtr.getStartOffset()], 0,
                            new byte[] { ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG });
                    PointableHelper.setNull(result);
                    return;
                }
                listAccessor.reset(valuePtr.getByteArray(), valuePtr.getStartOffset());
                int size = listAccessor.size();
                if (size == 0) {
                    ExceptionUtil.warnFunctionEvalFailed(ctx, sourceLoc, funcId,
                            "normalize-vector expects a non-empty numeric list");
                    PointableHelper.setNull(result);
                    return;
                }
                vector = decoder.createArrayFromList(listAccessor, decoder.ensureDoubleCapacity(vector, size));
                if (!VectorDistanceCalculation.normalizeInPlace(vector)) {
                    ExceptionUtil.warnFunctionEvalFailed(ctx, sourceLoc, funcId,
                            "normalize vector expects equal-length non-empty arrays");
                    PointableHelper.setNull(result);
                    return;
                }
                listBuilder.reset(DOUBLE_LIST);
                for (int i = 0; i < vector.length; i++) {
                    itemStorage.reset();
                    aDouble.setValue(vector[i]);
                    doubleSerde.serialize(aDouble, itemStorage.getDataOutput());
                    listBuilder.addItem(itemStorage);
                }
                listBuilder.write(out, true);
                result.set(resultStorage);
            } catch (IOException e) {
                ExceptionUtil.warnFunctionEvalFailed(ctx, sourceLoc, funcId, e.getMessage());
                PointableHelper.setNull(result);
            }
        }
    }
}
