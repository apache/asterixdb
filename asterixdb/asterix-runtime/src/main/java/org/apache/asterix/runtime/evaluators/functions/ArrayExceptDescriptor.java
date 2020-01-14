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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
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
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * array_except(arg1, arg2) takes 2 arguments, arg1 is an array, arg2 is an array, and returns an array with all
 * the items in arg1 array except for the items in arg2 array.
 */

@MissingNullInOutFunction
public class ArrayExceptDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY =
            DescriptorFactoryUtil.createFactory(ArrayExceptDescriptor::new, FunctionTypeInferers.SET_ARGUMENTS_TYPE);

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = (IAType[]) states;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_EXCEPT;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayExceptEval(args, ctx, sourceLoc, getIdentifier(), argTypes);
            }
        };
    }

    private class ArrayExceptEval extends AbstractScalarEval {

        private final IEvaluatorContext ctx;

        // Result
        private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        private final DataOutput storageOutput = storage.getDataOutput();

        // List items (result is always a list of type string)
        private final IAsterixListBuilder orderedListBuilder = new OrderedListBuilder();
        private final IAsterixListBuilder unorderedListBuilder = new UnorderedListBuilder();

        // Arguments
        private final IScalarEvaluator arg1Eval;
        private final IScalarEvaluator arg2Eval;
        private final IPointable arg1Pointable = new VoidPointable();
        private final IPointable arg2Pointable = new VoidPointable();

        // List items
        private final ListAccessor arg1ListAccessor = new ListAccessor();
        private final ListAccessor arg2ListAccessor = new ListAccessor();
        private IPointable arg1ListItem = new VoidPointable();
        private IPointable arg2ListItem = new VoidPointable();
        private ArrayBackedValueStorage arg1Storage = new ArrayBackedValueStorage();
        private ArrayBackedValueStorage arg2Storage = new ArrayBackedValueStorage();

        // Comparator
        private IBinaryComparator comparator;

        ArrayExceptEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, SourceLocation sourceLocation,
                FunctionIdentifier functionIdentifier, IAType[] argTypes) throws HyracksDataException {
            super(sourceLocation, functionIdentifier);

            this.ctx = ctx;

            arg1Eval = args[0].createScalarEvaluator(ctx);
            arg2Eval = args[1].createScalarEvaluator(ctx);

            IAType leftType =
                    (argTypes[0].getTypeTag() == ATypeTag.ARRAY || argTypes[0].getTypeTag() == ATypeTag.MULTISET)
                            ? ((AbstractCollectionType) argTypes[0]).getItemType() : BuiltinType.ANY;

            IAType rightType =
                    (argTypes[1].getTypeTag() == ATypeTag.ARRAY || argTypes[1].getTypeTag() == ATypeTag.MULTISET)
                            ? ((AbstractCollectionType) argTypes[1]).getItemType() : BuiltinType.ANY;

            comparator = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(leftType, rightType, true)
                    .createBinaryComparator();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            arg1Eval.evaluate(tuple, arg1Pointable);
            arg2Eval.evaluate(tuple, arg2Pointable);

            if (PointableHelper.checkAndSetMissingOrNull(result, arg1Pointable, arg2Pointable)) {
                return;
            }

            byte[] arg1Bytes = arg1Pointable.getByteArray();
            int arg1StartOffset = arg1Pointable.getStartOffset();
            ATypeTag arg1TypeTag = ATYPETAGDESERIALIZER.deserialize(arg1Bytes[arg1StartOffset]);

            byte[] arg2Bytes = arg2Pointable.getByteArray();
            int arg2StartOffset = arg2Pointable.getStartOffset();
            ATypeTag arg2TypeTag = ATYPETAGDESERIALIZER.deserialize(arg2Bytes[arg2StartOffset]);

            if (!arg1TypeTag.isListType()) {
                ExceptionUtil.warnTypeMismatch(ctx, srcLoc, funID, arg1Bytes[arg1StartOffset], 0, ATypeTag.ARRAY);
                PointableHelper.setNull(result);
                return;
            }

            if (!arg2TypeTag.isListType()) {
                ExceptionUtil.warnTypeMismatch(ctx, srcLoc, funID, arg2Bytes[arg2StartOffset], 1, ATypeTag.ARRAY);
                PointableHelper.setNull(result);
                return;
            }

            // Reset the lists
            arg1ListAccessor.reset(arg1Bytes, arg1StartOffset);
            arg2ListAccessor.reset(arg2Bytes, arg2StartOffset);

            // If any of the lists is empty, return the first list as is
            if (arg1ListAccessor.size() == 0 || arg2ListAccessor.size() == 0) {
                result.set(arg1Pointable);
                return;
            }

            // Builder collection type
            IAsterixListBuilder resultListBuilder =
                    arg1ListAccessor.getListType() == ATypeTag.ARRAY ? orderedListBuilder : unorderedListBuilder;
            AbstractCollectionType collectionType;

            // If it is not a known list type, use open type
            if (!argTypes[0].getTypeTag().isListType()) {
                collectionType = (AbstractCollectionType) DefaultOpenFieldType
                        .getDefaultOpenFieldType(arg1ListAccessor.getListType());
            } else {
                // known list type, use it
                collectionType = (AbstractCollectionType) argTypes[0];
            }

            resultListBuilder.reset(collectionType);

            try {
                // Filter the items
                boolean isItemFound;
                for (int i = 0; i < arg1ListAccessor.size(); i++) {
                    isItemFound = false;
                    arg1Storage.reset();
                    arg1ListAccessor.getOrWriteItem(i, arg1ListItem, arg1Storage);

                    // Check if the item exists in second list
                    for (int j = 0; j < arg2ListAccessor.size(); j++) {
                        arg2Storage.reset();
                        arg2ListAccessor.getOrWriteItem(j, arg2ListItem, arg2Storage);

                        // Found the item in second list
                        if (comparator.compare(arg1ListItem.getByteArray(), arg1ListItem.getStartOffset(),
                                arg1ListItem.getLength(), arg2ListItem.getByteArray(), arg2ListItem.getStartOffset(),
                                arg2ListItem.getLength()) == 0) {
                            isItemFound = true;
                            break;
                        }
                    }

                    // Add the item if it is not found
                    if (!isItemFound) {
                        resultListBuilder.addItem(arg1ListItem);
                    }
                }
            } catch (IOException ex) {
                throw HyracksDataException.create(ex);
            }

            storage.reset();
            resultListBuilder.write(storageOutput, true);
            result.set(storage);
        }
    }
}
