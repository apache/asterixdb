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

/**
 * <pre>
 * array_put(list, val1, val2, ...) returns a new list with all the values appended to the input list items only if
 * the list does not already have the value. Values cannot be null (i.e., one cannot append nulls).
 * array_put([2, 3], 2, 2, 9, 9) will result in [2, 3, 9, 9].
 *
 * It throws an error at compile time if the number of arguments < 2
 *
 * It returns (or throws an error at runtime) in order:
 * 1. missing, if any argument is missing.
 * 2. null, if any argument is null.
 * 3. otherwise, a new list.
 *
 * </pre>
 */
public class ArrayPutDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY =
            DescriptorFactoryUtil.createFactory(ArrayPutDescriptor::new, FunctionTypeInferers.SET_ARGUMENTS_TYPE);

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_PUT;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayPutEval(args, ctx);
            }
        };
    }

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = (IAType[]) states;
    }

    public class ArrayPutEval extends AbstractArrayAddRemoveEval {
        private final ArrayBackedValueStorage storage;
        private final IPointable item;
        private final IBinaryComparator comp;
        private final boolean[] add;

        ArrayPutEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            super(args, ctx, 0, 1, args.length - 1, argTypes, true, false);
            // input list will be opened since makeOpen=true. all values will be opened, too. hence ANY for comp.
            comp = BinaryComparatorFactoryProvider.INSTANCE
                    .getBinaryComparatorFactory(BuiltinType.ANY, BuiltinType.ANY, true).createBinaryComparator();
            storage = new ArrayBackedValueStorage();
            item = new VoidPointable();
            add = new boolean[args.length - 1];
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder, IPointable[] values,
                int position) throws IOException {
            markAllToBeAdded();
            // get the list items one by one and append to the new list
            for (int i = 0; i < listAccessor.size(); i++) {
                listAccessor.getOrWriteItem(i, item, storage);
                listBuilder.addItem(item);
                // mark the equal values to skip adding them
                for (int j = 0; j < values.length; j++) {
                    if (add[j] && comp.compare(item.getByteArray(), item.getStartOffset(), item.getLength(),
                            values[j].getByteArray(), values[j].getStartOffset(), values[j].getLength()) == 0) {
                        add[j] = false;
                    }
                    // skip comparison if the value is already marked
                }
            }
            // append the values arguments only if they are not already present in the list, i.e. not marked
            for (int i = 0; i < values.length; i++) {
                if (add[i]) {
                    listBuilder.addItem(values[i]);
                }
            }
        }

        private void markAllToBeAdded() {
            for (int i = 0; i < add.length; i++) {
                add[i] = true;
            }
        }
    }
}
