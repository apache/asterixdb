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
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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
 * 3. an error if any value arg is of a list/object type (i.e. derived type) since deep equality is not yet supported.
 * 4. otherwise, a new list.
 *
 * </pre>
 */
public class ArrayPutDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayPutDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENTS_TYPE;
        }
    };

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
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
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
        private final IBinaryComparator comp;

        public ArrayPutEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx) throws HyracksDataException {
            super(args, ctx, 0, 1, args.length - 1, argTypes, true, sourceLoc, true, false);
            comp = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            storage = new ArrayBackedValueStorage();
        }

        @Override
        protected int getPosition(IFrameTupleReference tuple, IPointable l, ATypeTag listTag)
                throws HyracksDataException {
            // l = list
            if (listTag == ATypeTag.ARRAY) {
                return AOrderedListSerializerDeserializer.getNumberOfItems(l.getByteArray(), l.getStartOffset());
            } else if (listTag == ATypeTag.MULTISET) {
                return AUnorderedListSerializerDeserializer.getNumberOfItems(l.getByteArray(), l.getStartOffset());
            } else {
                return RETURN_NULL;
            }
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder, IPointable[] values,
                int position) throws IOException {
            boolean[] dontAdd = new boolean[values.length];
            // get the list items one by one and append to the new list
            for (int i = 0; i < listAccessor.size(); i++) {
                storage.reset();
                listAccessor.writeItem(i, storage.getDataOutput());
                listBuilder.addItem(storage);
                // mark the equal values to skip adding them
                for (int j = 0; j < values.length; j++) {
                    if (!dontAdd[j]
                            && comp.compare(storage.getByteArray(), storage.getStartOffset(), storage.getLength(),
                                    values[j].getByteArray(), values[j].getStartOffset(), values[j].getLength()) == 0) {
                        dontAdd[j] = true;
                    }
                    // skip comparison if the value is already marked
                }
            }
            // append the values arguments only if they are not already present in the list, i.e. not marked
            for (int i = 0; i < values.length; i++) {
                if (!dontAdd[i]) {
                    listBuilder.addItem(values[i]);
                }
            }
        }
    }
}
