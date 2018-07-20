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
 * array_remove(list, val1, val2, ...) returns a new (open or closed) list with all the values removed from the input
 * list. Values cannot be null (i.e., one cannot remove nulls).
 *
 * It throws an error at compile time if the number of arguments < 2
 *
 * It returns (or throws an error at runtime) in order:
 * 1. missing, if any argument is missing.
 * 2. null, if any argument is null.
 * 4. an error if any value arg is of a list/object type (i.e. derived type) since deep equality is not yet supported.
 * 3. otherwise, a new list that has the same type as the input list.
 *
 * </pre>
 */
public class ArrayRemoveDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayRemoveDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENTS_TYPE;
        }
    };

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
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new ArrayRemoveEval(args, ctx);
            }
        };
    }

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = (IAType[]) states;
    }

    public class ArrayRemoveEval extends AbstractArrayAddRemoveEval {
        private final ArrayBackedValueStorage storage;
        private final IBinaryComparator comp;

        public ArrayRemoveEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx) throws HyracksDataException {
            super(args, ctx, 0, 1, args.length - 1, argTypes, true, sourceLoc, false, false);
            storage = new ArrayBackedValueStorage();
            comp = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        }

        @Override
        protected int getPosition(IFrameTupleReference tuple, IPointable listArg, ATypeTag listTag)
                throws HyracksDataException {
            return 0;
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder, IPointable[] removed,
                int position) throws IOException {
            // get the list items one by one and append to the new list only if the list item is not in removed list
            boolean addItem;
            for (int i = 0; i < listAccessor.size(); i++) {
                storage.reset();
                listAccessor.writeItem(i, storage.getDataOutput());
                addItem = true;
                for (int j = 0; j < removed.length; j++) {
                    if (comp.compare(storage.getByteArray(), storage.getStartOffset(), storage.getLength(),
                            removed[j].getByteArray(), removed[j].getStartOffset(), removed[j].getLength()) == 0) {
                        addItem = false;
                        break;
                    }
                }
                if (addItem) {
                    listBuilder.addItem(storage);
                }
            }
        }
    }
}
