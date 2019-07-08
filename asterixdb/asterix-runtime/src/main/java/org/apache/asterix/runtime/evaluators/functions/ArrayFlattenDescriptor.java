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

import java.io.IOException;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.base.ListAccessorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * <pre>
 * array_flatten(list, depth) returns a new list with any nested list (all types) flattened up to the specified
 * depth. The returned list type is the same as the input list type. Null and missing items are preserved.
 * If the depth < 0, then it flattens the input list all the way deep.
 *
 * array_flatten([2, null, [5,6], 3, missing], 1) will result in [2, null, 5, 6, 3, null]
 * array_flatten([2, [5,6], 3], 0) will result in [2, [5,6], 3] (0 depth does nothing)
 *
 * It throws an error at compile time if the number of arguments != 2
 *
 * It returns in order:
 * 1. missing, if any argument is missing.
 * 2. null, if:
 * - any argument is null.
 * - the input list is not a list.
 * - the depth arg is not numeric or
 * - it's a floating-point number with decimals (e.g. 1.2 will produce null, 1.0 is OK).
 * 3. otherwise, a new list.
 *
 * </pre>
 */

@MissingNullInOutFunction
public class ArrayFlattenDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType inputListType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayFlattenDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_FLATTEN;
    }

    @Override
    public void setImmutableStates(Object... states) {
        inputListType = (IAType) states[0];
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayFlattenEval(args, ctx);
            }
        };
    }

    public class ArrayFlattenEval implements IScalarEvaluator {
        private final IScalarEvaluator listEval;
        private final IScalarEvaluator depthEval;
        private final IPointable list;
        private final AbstractPointable pointable;
        private final TaggedValuePointable depthArg;
        private final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;
        private final IObjectPool<ListAccessor, ATypeTag> listAccessorAllocator;
        private final CastTypeEvaluator caster;
        private final ArrayBackedValueStorage finalStorage;
        private ArrayBackedValueStorage storage;
        private IAsterixListBuilder orderedListBuilder;
        private IAsterixListBuilder unorderedListBuilder;

        public ArrayFlattenEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
            listAccessorAllocator = new ListObjectPool<>(new ListAccessorFactory());
            finalStorage = new ArrayBackedValueStorage();
            listEval = args[0].createScalarEvaluator(ctx);
            depthEval = args[1].createScalarEvaluator(ctx);
            list = new VoidPointable();
            pointable = new VoidPointable();
            caster = new CastTypeEvaluator();
            depthArg = new TaggedValuePointable();
            orderedListBuilder = null;
            unorderedListBuilder = null;
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            // 1st arg: list to flatten
            listEval.evaluate(tuple, pointable);
            // 2nd arg: depthArg
            depthEval.evaluate(tuple, depthArg);

            if (PointableHelper.checkAndSetMissingOrNull(result, pointable, depthArg)) {
                return;
            }

            ATypeTag listType = ATYPETAGDESERIALIZER.deserialize(pointable.getByteArray()[pointable.getStartOffset()]);
            if (!ATypeHierarchy.isCompatible(ATYPETAGDESERIALIZER.deserialize(depthArg.getTag()), ATypeTag.DOUBLE)
                    || !listType.isListType()) {
                PointableHelper.setNull(result);
                return;
            }
            String name = getIdentifier().getName();
            double depth = ATypeHierarchy.getDoubleValue(name, 1, depthArg.getByteArray(), depthArg.getStartOffset());
            if (Double.isNaN(depth) || Double.isInfinite(depth) || Math.floor(depth) < depth) {
                PointableHelper.setNull(result);
                return;
            }

            try {
                caster.resetAndAllocate(DefaultOpenFieldType.getDefaultOpenFieldType(listType), inputListType,
                        listEval);
                caster.cast(pointable, list);

                int depthInt = (int) depth;
                // create list
                IAsterixListBuilder listBuilder;
                if (listType == ATypeTag.ARRAY) {
                    if (orderedListBuilder == null) {
                        orderedListBuilder = new OrderedListBuilder();
                    }
                    listBuilder = orderedListBuilder;
                } else {
                    if (unorderedListBuilder == null) {
                        unorderedListBuilder = new UnorderedListBuilder();
                    }
                    listBuilder = unorderedListBuilder;
                }

                ListAccessor mainListAccessor = listAccessorAllocator.allocate(null);
                listBuilder.reset((AbstractCollectionType) DefaultOpenFieldType.getDefaultOpenFieldType(listType));
                mainListAccessor.reset(list.getByteArray(), list.getStartOffset());

                storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                process(mainListAccessor, listBuilder, 0, depthInt);
                finalStorage.reset();
                listBuilder.write(finalStorage.getDataOutput(), true);
                result.set(finalStorage);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            } finally {
                storageAllocator.reset();
                listAccessorAllocator.reset();
                caster.deallocatePointables();
            }
        }

        private void process(ListAccessor listAccessor, IAsterixListBuilder listBuilder, int currentDepth, int depth)
                throws IOException {
            boolean itemInStorage;
            for (int i = 0; i < listAccessor.size(); i++) {
                itemInStorage = listAccessor.getOrWriteItem(i, pointable, storage);
                // if item is not a list or depth is reached, write it
                if (!ATYPETAGDESERIALIZER.deserialize(pointable.getByteArray()[pointable.getStartOffset()]).isListType()
                        || currentDepth == depth) {
                    listBuilder.addItem(pointable);
                } else {
                    // recurse on the sublist
                    ListAccessor newListAccessor = listAccessorAllocator.allocate(null);
                    newListAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
                    if (itemInStorage) {
                        // create a new storage since the item is using it
                        storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                        storage.reset();
                    }
                    process(newListAccessor, listBuilder, currentDepth + 1, depth);
                }
            }
        }
    }
}
