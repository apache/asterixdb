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
import java.util.Arrays;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ArrayAppendDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayAppendDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENTS_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_APPEND;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new ArrayAppendFunction(args, ctx);
            }
        };
    }

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = Arrays.copyOf(states, states.length, IAType[].class);
    }

    public class ArrayAppendFunction implements IScalarEvaluator {
        private final ArrayBackedValueStorage storage;
        private final IPointable listArg;
        private final IPointable[] appendedValues;
        private final IScalarEvaluator listArgEval;
        private final IScalarEvaluator[] appendedValuesEval;

        public ArrayAppendFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx)
                throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            listArg = new VoidPointable();
            listArgEval = args[0].createScalarEvaluator(ctx);
            appendedValues = new IPointable[args.length - 1];
            appendedValuesEval = new IScalarEvaluator[args.length - 1];
            for (int i = 1; i < args.length; i++) {
                appendedValues[i - 1] = new VoidPointable();
                appendedValuesEval[i - 1] = args[i].createScalarEvaluator(ctx);
            }
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            // get the list argument, 1st argument, make sure it's a list
            listArgEval.evaluate(tuple, listArg);
            byte listArgType = listArg.getByteArray()[listArg.getStartOffset()];

            CastTypeEvaluator caster = null;
            AbstractCollectionType listType = null;
            IAsterixListBuilder listBuilder = null;
            // create the new list to be returned. The item type is always "ANY"
            // cast the input list and make it open
            if (listArgType == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                listBuilder = new OrderedListBuilder();
                listType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                caster = new CastTypeEvaluator(listType, argTypes[0], listArgEval);
                caster.evaluate(tuple, listArg);
            } else if (listArgType == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                listBuilder = new UnorderedListBuilder();
                listType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
                caster = new CastTypeEvaluator(listType, argTypes[0], listArgEval);
                caster.evaluate(tuple, listArg);
            }
            // else, don't return null right away. evaluate rest of args as some may be missing, return missing instead
            IAType defaultOpenType;
            for (int i = 0; i < appendedValuesEval.length; i++) {
                // cast to open if necessary
                defaultOpenType = DefaultOpenFieldType.getDefaultOpenFieldType(argTypes[i + 1].getTypeTag());
                if (defaultOpenType != null && caster != null) {
                    caster.reset(defaultOpenType, argTypes[i + 1], appendedValuesEval[i]);
                    caster.evaluate(tuple, appendedValues[i]);
                } else {
                    // either no casting is needed (e.g. int and the like) or evaluate normally for the below case:
                    // when caster == null, it means the first arg was not a list and a null would be returned but
                    // evaluate values to be appended normally in case missing exists and return missing instead of null
                    appendedValuesEval[i].evaluate(tuple, appendedValues[i]);
                }
            }

            if (!ATYPETAGDESERIALIZER.deserialize(listArgType).isListType()) {
                PointableHelper.setNull(result);
                return;
            }

            // arguments are good: no nulls/missings and 1st arg is a list
            listBuilder.reset(listType);
            ListAccessor listAccessor = new ListAccessor();
            listAccessor.reset(listArg.getByteArray(), listArg.getStartOffset());
            try {
                // get the list items one by one and append to the new list
                for (int i = 0; i < listAccessor.size(); i++) {
                    storage.reset();
                    listAccessor.writeItem(i, storage.getDataOutput());
                    listBuilder.addItem(storage);
                }
                // append the values arguments
                for (IPointable appendedValue : appendedValues) {
                    listBuilder.addItem(appendedValue);
                }
                storage.reset();
                listBuilder.write(storage.getDataOutput(), true);
                result.set(storage);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
