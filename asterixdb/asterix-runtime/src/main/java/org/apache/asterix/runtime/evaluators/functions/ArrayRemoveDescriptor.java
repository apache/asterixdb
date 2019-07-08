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
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * <pre>
 * array_remove(list, val1, val2, ...) returns a new list with all the values removed from the input
 * list. Values cannot be null (i.e., one cannot remove nulls). It's case-sensitive to string value arguments.
 *
 * It throws an error at compile time if the number of arguments < 2
 *
 * It returns (or throws an error at runtime) in order:
 * 1. missing, if any argument is missing.
 * 2. null, if any argument is null.
 * 3. otherwise, a new list that has the same type as the input list.
 *
 * </pre>
 */
public class ArrayRemoveDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY =
            DescriptorFactoryUtil.createFactory(ArrayRemoveDescriptor::new, FunctionTypeInferers.SET_ARGUMENTS_TYPE);

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_REMOVE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayRemoveEval(args, ctx, argTypes);
            }
        };
    }

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = (IAType[]) states;
    }

    public static class ArrayRemoveEval extends AbstractArrayAddRemoveEval {
        private final ArrayBackedValueStorage storage;
        private final IPointable item;
        private final IBinaryComparator[] comp;

        ArrayRemoveEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, IAType[] argTypes)
                throws HyracksDataException {
            super(args, ctx, 0, 1, args.length - 1, argTypes, false, false);
            storage = new ArrayBackedValueStorage();
            item = new VoidPointable();
            comp = createValuesComparators(argTypes);
        }

        @Override
        protected int getPosition(IFrameTupleReference tuple, IPointable listArg, ATypeTag listTag) {
            return 0;
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder, IPointable[] removed,
                int position) throws IOException {
            // get the list items one by one and append to the new list only if the list item is not in removed list
            boolean addItem;
            for (int i = 0; i < listAccessor.size(); i++) {
                listAccessor.getOrWriteItem(i, item, storage);
                addItem = true;
                for (int j = 0; j < removed.length; j++) {
                    if (comp[j].compare(item.getByteArray(), item.getStartOffset(), item.getLength(),
                            removed[j].getByteArray(), removed[j].getStartOffset(), removed[j].getLength()) == 0) {
                        addItem = false;
                        break;
                    }
                }
                if (addItem) {
                    listBuilder.addItem(item);
                }
            }
        }

        private static IBinaryComparator[] createValuesComparators(IAType[] argTypes) {
            // one comparator for each value since the values will not be opened (they won't be added to a list).
            // for the list item, it's either the item type if input list is determined to be a list or ANY if not.
            IAType itemType = argTypes[0].getTypeTag().isListType()
                    ? ((AbstractCollectionType) argTypes[0]).getItemType() : BuiltinType.ANY;
            IBinaryComparator[] comparators = new IBinaryComparator[argTypes.length - 1];
            for (int i = 1; i < argTypes.length; i++) {
                comparators[i - 1] = BinaryComparatorFactoryProvider.INSTANCE
                        .getBinaryComparatorFactory(itemType, argTypes[i], true).createBinaryComparator();
            }
            return comparators;
        }
    }
}
