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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * <pre>
 * array_ifnull(list) returns the first item it encounters that is not a null or missing. Otherwise, it returns null.
 *
 * It throws an error at compile time if the number of arguments != 1
 *
 * It returns in order:
 * 1. missing if the input list is missing
 * 2. null if the input list is null or is not a list.
 * 3. otherwise, the first non-null non-missing item in the list. Otherwise, null.
 *
 * </pre>
 */

@MissingNullInOutFunction
public class ArrayIfNullDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayIfNullDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_IFNULL;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayIfNullEval(args, ctx);
            }
        };
    }

    public class ArrayIfNullEval implements IScalarEvaluator {
        private final ArrayBackedValueStorage storage;
        private final IScalarEvaluator listArgEval;
        private final IPointable listArg;
        private final ListAccessor listAccessor;
        private final AbstractPointable item;

        public ArrayIfNullEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            listArg = new VoidPointable();
            item = new VoidPointable();
            listAccessor = new ListAccessor();
            listArgEval = args[0].createScalarEvaluator(ctx);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            // get the list argument and make sure it's a list
            listArgEval.evaluate(tuple, listArg);

            if (PointableHelper.checkAndSetMissingOrNull(result, listArg)) {
                return;
            }

            byte[] listBytes = listArg.getByteArray();
            int offset = listArg.getStartOffset();
            ATypeTag listType = ATYPETAGDESERIALIZER.deserialize(listBytes[offset]);
            if (!listType.isListType()) {
                PointableHelper.setNull(result);
                return;
            }

            listAccessor.reset(listBytes, offset);
            ATypeTag itemTypeTag = listAccessor.getItemType();
            try {
                if (itemTypeTag == ATypeTag.NULL || itemTypeTag == ATypeTag.MISSING) {
                    // list of nulls or list of missings
                    PointableHelper.setNull(result);
                    return;
                }

                int numItems = listAccessor.size();
                for (int i = 0; i < numItems; i++) {
                    listAccessor.getOrWriteItem(i, item, storage);
                    itemTypeTag = ATYPETAGDESERIALIZER.deserialize(item.getByteArray()[item.getStartOffset()]);
                    if (itemTypeTag != ATypeTag.NULL && itemTypeTag != ATypeTag.MISSING) {
                        result.set(item);
                        return;
                    }
                }
                PointableHelper.setNull(result);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
