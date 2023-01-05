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
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * array_swap(list, position1, position2) returns a new list, switching the positions of both the items in position1
 * and position2.
 *
 * It returns in order:
 * Missing, if any of the input arguments are missing.
 * Null, if the arguments are null, if the list argument is not a list, or if the positional arguments is not numerical.
 * Otherwise, it returns a new list, where the two items at the
 */

public class ArraySwapDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private IAType inputListType;
    private String funcIDString = String.valueOf(BuiltinFunctions.ARRAY_SWAP);

    public static final IFunctionDescriptorFactory FACTORY =
            DescriptorFactoryUtil.createFactory(ArraySwapDescriptor::new, FunctionTypeInferers.SET_ARGUMENTS_TYPE);

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_SWAP;
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
                return new ArraySwapDescriptor.ArraySwapEval(args, ctx);
            }
        };
    }

    public class ArraySwapEval extends AbstractArrayMoveSwapEval {
        private final ArrayBackedValueStorage storage;

        ArraySwapEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            super(args, ctx, funcIDString, inputListType);
            storage = new ArrayBackedValueStorage();
        }

        @Override
        protected void buildList(int oldIndexInt, int newIndexInt, int listLen, ListAccessor listAccessor,
                IAsterixListBuilder listBuilder) throws IOException {
            for (int i = 0; i < listLen; i++) {
                if (oldIndexInt < newIndexInt) {
                    if (i == oldIndexInt) {
                        storage.reset();
                        listAccessor.writeItem(newIndexInt, storage.getDataOutput());
                        listBuilder.addItem(storage);
                    }
                    // the other case is when it is one of the two input indices, in which case we want to swap the two
                    else if (i == newIndexInt) {
                        storage.reset();
                        listAccessor.writeItem(oldIndexInt, storage.getDataOutput());
                        listBuilder.addItem(storage);
                    } else {
                        storage.reset();
                        listAccessor.writeItem(i, storage.getDataOutput());
                        listBuilder.addItem(storage);
                    }
                } else if (oldIndexInt > newIndexInt) {
                    if (i == newIndexInt) {
                        storage.reset();
                        listAccessor.writeItem(oldIndexInt, storage.getDataOutput());
                        listBuilder.addItem(storage);
                    } else if (i == oldIndexInt) {
                        storage.reset();
                        listAccessor.writeItem(newIndexInt, storage.getDataOutput());
                        listBuilder.addItem(storage);
                    } else {
                        storage.reset();
                        listAccessor.writeItem(i, storage.getDataOutput());
                        listBuilder.addItem(storage);
                    }
                }
            }
        }

    }

}
