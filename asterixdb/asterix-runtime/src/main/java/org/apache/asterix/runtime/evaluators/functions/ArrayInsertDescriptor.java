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

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * <pre>
 * array_insert(list, pos, val1, val2, ...) returns a new list with all values inserted at the specified position.
 * Values can be null (i.e., one can insert nulls). Position can be negative where the last position = -1. When position
 * is positive then the first position = 0. Input list can be empty where the only valid position is 0.
 * For the list [5,6], the valid positions are 0, 1, 2, -1, -2. If position is floating-point, it's casted to integer.
 *
 * It throws an error at compile time if the number of arguments < 3
 *
 * It returns in order:
 * 1. missing, if any argument is missing.
 * 2. null, if
 * - the list arg is null or it's not a list
 * - the position is not numeric or the position is out of bound or it's a floating-point with decimals or NaN or +-INF.
 * 3. otherwise, a new list.
 *
 * </pre>
 */
public class ArrayInsertDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY =
            DescriptorFactoryUtil.createFactory(ArrayInsertDescriptor::new, FunctionTypeInferers.SET_ARGUMENTS_TYPE);

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_INSERT;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayInsertEval(args, ctx);
            }
        };
    }

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = (IAType[]) states;
    }

    public class ArrayInsertEval extends AbstractArrayAddRemoveEval {
        private final TaggedValuePointable positionArg;
        private final IScalarEvaluator positionArgEval;

        ArrayInsertEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            super(args, ctx, 0, 2, args.length - 2, argTypes, true, true);
            positionArg = new TaggedValuePointable();
            positionArgEval = args[1].createScalarEvaluator(ctx);
        }

        @Override
        protected int getPosition(IFrameTupleReference tuple, IPointable l, ATypeTag listTag)
                throws HyracksDataException {
            // TODO(ali): could be optimized to evaluate only once if we know the argument is a constant
            positionArgEval.evaluate(tuple, positionArg);
            if (positionArg.getTag() == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                return RETURN_MISSING;
            }

            double position;
            if (!ATypeHierarchy.isCompatible(ATypeTag.DOUBLE, ATYPETAGDESERIALIZER.deserialize(positionArg.getTag()))
                    || !listTag.isListType()) {
                return RETURN_NULL;
            } else {
                String name = getIdentifier().getName();
                position = ATypeHierarchy.getDoubleValue(name, 1, positionArg.getByteArray(),
                        positionArg.getStartOffset());
                if (Double.isNaN(position) || Double.isInfinite(position) || Math.floor(position) < position) {
                    return RETURN_NULL;
                }
                // list size
                int size;
                if (listTag == ATypeTag.ARRAY) {
                    size = AOrderedListSerializerDeserializer.getNumberOfItems(l.getByteArray(), l.getStartOffset());
                } else {
                    size = AUnorderedListSerializerDeserializer.getNumberOfItems(l.getByteArray(), l.getStartOffset());
                }
                // adjust position for negative positions
                if (position < 0) {
                    position = size + position;
                }
                // position should always be positive now and should be within [0-list_size]
                if (position < 0 || position > size) {
                    return RETURN_NULL;
                }
                return (int) position;
            }
        }
    }
}
