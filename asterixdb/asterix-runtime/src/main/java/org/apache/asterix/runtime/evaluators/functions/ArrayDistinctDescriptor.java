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

import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.utils.PointableHashSet;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * <pre>
 * array_distinct(list) returns a new list with distinct items of the input list. The returned list has the same type as
 * the input list. The list can contain null and missing items. Null and missing are considered to be the same.
 * It's case-sensitive to string items.
 *
 * array_distinct([1,2,null,4,missing,2,1]) will output [1,2,null,4]
 *
 * It throws an error at compile time if the number of arguments != 1
 *
 * It returns (or throws an error at runtime) in order:
 * 1. missing, if any argument is missing.
 * 2. null, if the list arg is null or it's not a list.
 * 3. otherwise, a new list.
 *
 * </pre>
 */

@MissingNullInOutFunction
public class ArrayDistinctDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType inputListType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayDistinctDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            // the type of the input list is needed in order to use the same type for the new returned list
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_DISTINCT;
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
                return new ArrayDistinctFunction(args, ctx);
            }
        };
    }

    public class ArrayDistinctFunction extends AbstractArrayProcessEval {
        private final PointableHashSet itemSet;
        private IPointable item;
        private ArrayBackedValueStorage storage;

        ArrayDistinctFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            super(args, ctx, inputListType);
            IAType itemType = inputListType.getTypeTag().isListType()
                    ? ((AbstractCollectionType) inputListType).getItemType() : BuiltinType.ANY;
            itemSet = new PointableHashSet(arrayListAllocator, itemType);
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder) throws IOException {
            itemSet.clear();
            item = pointableAllocator.allocateEmpty();
            storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
            boolean nullMissingWasAdded = false;
            for (int i = 0; i < listAccessor.size(); i++) {
                // get the item and compute its hash
                boolean itemInStorage = listAccessor.getOrWriteItem(i, item, storage);
                if (isNullOrMissing(item)) {
                    if (!nullMissingWasAdded) {
                        listBuilder.addItem(item);
                        nullMissingWasAdded = true;
                    }
                } else if (itemSet.add(item)) {
                    listBuilder.addItem(item);
                    item = pointableAllocator.allocateEmpty();
                    if (itemInStorage) {
                        // create new storage since the added item is using it now
                        storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                    }
                }
            }
        }

        private boolean isNullOrMissing(IPointable item) {
            byte tag = item.getByteArray()[item.getStartOffset()];
            return tag == ATypeTag.SERIALIZED_NULL_TYPE_TAG || tag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG;
        }
    }
}
