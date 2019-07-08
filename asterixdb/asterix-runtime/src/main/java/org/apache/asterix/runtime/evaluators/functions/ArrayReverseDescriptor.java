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
import org.apache.asterix.om.types.IAType;
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
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * <pre>
 * array_reverse(list) returns a new list with the entries of the original input list in reverse order.
 * The returned list has the same type as the input list. The list can contain null/missing items. Both are preserved.
 *
 * It throws an error at compile time if the number of arguments != 1
 *
 * It returns in order:
 * 1. missing, if any argument is missing.
 * 2. null, if the list arg is null or it's not a list.
 * 3. otherwise, a new list.
 *
 * </pre>
 */

@MissingNullInOutFunction
public class ArrayReverseDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType inputListType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayReverseDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            // the type of the input list is needed in order to use the same type for the new returned list
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_REVERSE;
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
                return new ArrayReverseEval(args, ctx);
            }
        };
    }

    public class ArrayReverseEval extends AbstractArrayProcessEval {
        private final ArrayBackedValueStorage storage;
        private final AbstractPointable item;

        public ArrayReverseEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            super(args, ctx, inputListType);
            storage = new ArrayBackedValueStorage();
            item = new VoidPointable();
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder) throws IOException {
            // get the list items in reverse and append to the new list
            for (int i = listAccessor.size() - 1; i >= 0; i--) {
                listAccessor.getOrWriteItem(i, item, storage);
                listBuilder.addItem(item);
            }
        }
    }
}
