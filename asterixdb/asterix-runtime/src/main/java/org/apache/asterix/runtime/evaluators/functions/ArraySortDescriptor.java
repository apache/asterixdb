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
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * <pre>
 * array_sort(list) returns a new list with the items sorted in ascending order. The returned list has the same type as
 * the input list. The list can contain null and missing items, and both are preserved. It's case-sensitive to string
 * items.
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
public class ArraySortDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType inputListType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArraySortDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            // the type of the input list is needed in order to use the same type for the new returned list
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_SORT;
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
                return new ArraySortEval(args, ctx);
            }
        };
    }

    protected class ArraySortComparator implements Comparator<IPointable> {
        private final IBinaryComparator comp;

        ArraySortComparator(IAType itemType) {
            comp = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(itemType, itemType, true)
                    .createBinaryComparator();
        }

        @Override
        public int compare(IPointable val1, IPointable val2) {
            try {
                return comp.compare(val1.getByteArray(), val1.getStartOffset(), val1.getLength(), val2.getByteArray(),
                        val2.getStartOffset(), val2.getLength());
            } catch (HyracksDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public class ArraySortEval extends AbstractArrayProcessEval {
        private final PriorityQueue<IPointable> sortedList;
        private IPointable item;
        private ArrayBackedValueStorage storage;

        ArraySortEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            super(args, ctx, inputListType);
            IAType itemType = inputListType.getTypeTag().isListType()
                    ? ((AbstractCollectionType) inputListType).getItemType() : BuiltinType.ANY;
            sortedList = new PriorityQueue<>(new ArraySortComparator(itemType));
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder) throws IOException {
            sortedList.clear();
            boolean itemInStorage;
            item = pointableAllocator.allocateEmpty();
            storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
            for (int i = 0; i < listAccessor.size(); i++) {
                itemInStorage = listAccessor.getOrWriteItem(i, item, storage);
                sortedList.add(item);
                if (itemInStorage) {
                    storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                }
                item = pointableAllocator.allocateEmpty();
            }
            while (!sortedList.isEmpty()) {
                listBuilder.addItem(sortedList.poll());
            }
        }
    }
}
