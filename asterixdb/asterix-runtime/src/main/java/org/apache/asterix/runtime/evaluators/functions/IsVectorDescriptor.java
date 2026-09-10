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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.VectorValidator;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * {@code isvector(v)} and {@code isvector(v, dimension)}: true when {@code v} is a list whose every element
 * is numeric and whose length matches the given dimension.
 * <p>
 * The index build applies this same predicate, so a VTREE index contains exactly the rows for which
 * {@code isvector(field, dimension)} is true. That lets a user account for what was skipped:
 *
 * <pre>
 * SELECT COUNT(*) FROM ds WHERE NOT if_missing_or_null(isvector(ds.emb, 384), false);
 * </pre>
 * <p>
 * Null and missing propagate as for {@code is_array} and the rest of the {@code is_*} family, so
 * {@code isvector(null)} is {@code null} rather than {@code false}. Negating the call alone would
 * therefore leave out the rows whose field is null or absent, which the index skips too.
 * <p>
 * The function is <b>total</b> since {@link VectorValidator} never throws, which CLUSTER BY's usable-vector
 * guard relies on.
 */
@MissingNullInOutFunction
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class IsVectorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = IsVectorDescriptor::new;
    private static final long serialVersionUID = 1L;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IsVectorEvaluator(args, ctx);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.IS_VECTOR;
    }

    /**
     * {@code isvector(v, dimension)}. Registered under its own arity, so a call with any other argument
     * count does not resolve and null or missing propagates from either argument.
     */
    @MissingNullInOutFunction
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public static final class IsVectorWithDimensionDescriptor extends IsVectorDescriptor {
        public static final IFunctionDescriptorFactory FACTORY = IsVectorWithDimensionDescriptor::new;
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionIdentifier getIdentifier() {
            return BuiltinFunctions.IS_VECTOR_WITH_DIMENSION;
        }
    }

    private static final class IsVectorEvaluator implements IScalarEvaluator {

        private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
        private final DataOutput out = resultStorage.getDataOutput();
        private final IPointable valuePtr = new VoidPointable();
        private final IPointable dimensionPtr = new VoidPointable();
        private final IScalarEvaluator valueEval;
        private final IScalarEvaluator dimensionEval;
        private final VectorValidator validator = new VectorValidator();

        private IsVectorEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            valueEval = args[0].createScalarEvaluator(ctx);
            dimensionEval = args.length > 1 ? args[1].createScalarEvaluator(ctx) : null;
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            valueEval.evaluate(tuple, valuePtr);
            if (dimensionEval == null) {
                if (PointableHelper.checkAndSetMissingOrNull(result, valuePtr)) {
                    return;
                }
            } else {
                dimensionEval.evaluate(tuple, dimensionPtr);
                if (PointableHelper.checkAndSetMissingOrNull(result, valuePtr, dimensionPtr)) {
                    return;
                }
            }
            // Any dimension when none was given; getIntegerValue rejects a non-numeric second argument.
            int dimension = dimensionEval == null ? -1
                    : ATypeHierarchy.getIntegerValue(BuiltinFunctions.IS_VECTOR.getName(), 1,
                            dimensionPtr.getByteArray(), dimensionPtr.getStartOffset());
            boolean isVector = validator.isVector(valuePtr.getByteArray(), valuePtr.getStartOffset(),
                    valuePtr.getLength(), dimension);
            resultStorage.reset();
            AObjectSerializerDeserializer.INSTANCE.serialize(isVector ? ABoolean.TRUE : ABoolean.FALSE, out);
            result.set(resultStorage);
        }
    }
}
