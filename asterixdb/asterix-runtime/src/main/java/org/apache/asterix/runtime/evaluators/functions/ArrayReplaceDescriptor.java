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

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
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
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ArrayReplaceDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType inputListType;
    private IAType newValueType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayReplaceDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENTS_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_REPLACE;
    }

    @Override
    public void setImmutableStates(Object... states) {
        inputListType = (IAType) states[0];
        newValueType = (IAType) states[2];
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new ArrayReplaceEval(args, ctx);
            }
        };
    }

    public class ArrayReplaceEval implements IScalarEvaluator {
        private final IScalarEvaluator listEval;
        private final IScalarEvaluator targetValEval;
        private final IScalarEvaluator newValEval;
        private IScalarEvaluator maxEval;
        private final IPointable list;
        private final IPointable target;
        private final IPointable newVal;
        private TaggedValuePointable maxArg;
        private final AbstractPointable item;
        private final ListAccessor listAccessor;
        private final IBinaryComparator comp;
        private final ArrayBackedValueStorage storage;
        private final CastTypeEvaluator caster;
        private IAsterixListBuilder orderedListBuilder;
        private IAsterixListBuilder unorderedListBuilder;

        public ArrayReplaceEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx) throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            listEval = args[0].createScalarEvaluator(ctx);
            targetValEval = args[1].createScalarEvaluator(ctx);
            newValEval = args[2].createScalarEvaluator(ctx);
            if (args.length == 4) {
                maxEval = args[3].createScalarEvaluator(ctx);
                maxArg = new TaggedValuePointable();
            }
            list = new VoidPointable();
            target = new VoidPointable();
            newVal = new VoidPointable();
            item = new VoidPointable();
            listAccessor = new ListAccessor();
            caster = new CastTypeEvaluator();
            orderedListBuilder = null;
            unorderedListBuilder = null;
            comp = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            storage.reset();
            listEval.evaluate(tuple, list);
            targetValEval.evaluate(tuple, target);
            newValEval.evaluate(tuple, newVal);
            ATypeTag listType = ATYPETAGDESERIALIZER.deserialize(list.getByteArray()[list.getStartOffset()]);
            ATypeTag targetTag = ATYPETAGDESERIALIZER.deserialize(target.getByteArray()[target.getStartOffset()]);
            ATypeTag newValTag = ATYPETAGDESERIALIZER.deserialize(newVal.getByteArray()[newVal.getStartOffset()]);
            if (listType == ATypeTag.MISSING || targetTag == ATypeTag.MISSING || newValTag == ATypeTag.MISSING) {
                PointableHelper.setMissing(result);
                return;
            }

            double maxDouble = -1;
            String name = getIdentifier().getName();
            if (maxEval != null) {
                maxEval.evaluate(tuple, maxArg);
                ATypeTag maxTag = ATYPETAGDESERIALIZER.deserialize(maxArg.getTag());
                if (maxTag == ATypeTag.MISSING) {
                    PointableHelper.setMissing(result);
                    return;
                } else if (!ATypeHierarchy.isCompatible(maxTag, ATypeTag.DOUBLE)) {
                    PointableHelper.setNull(result);
                    return;
                }
                maxDouble = ATypeHierarchy.getDoubleValue(name, 3, maxArg.getByteArray(), maxArg.getStartOffset());
            }

            if (!listType.isListType() || Math.floor(maxDouble) < maxDouble || targetTag == ATypeTag.NULL
                    || Double.isInfinite(maxDouble) || Double.isNaN(maxDouble)) {
                PointableHelper.setNull(result);
                return;
            }

            if (targetTag.isDerivedType()) {
                throw new RuntimeDataException(ErrorCode.CANNOT_COMPARE_COMPLEX, sourceLoc);
            }

            IAType defaultOpenType = DefaultOpenFieldType.getDefaultOpenFieldType(listType);
            caster.reset(defaultOpenType, inputListType, listEval);
            caster.evaluate(tuple, list);

            defaultOpenType = DefaultOpenFieldType.getDefaultOpenFieldType(newValTag);
            if (defaultOpenType != null) {
                caster.reset(defaultOpenType, newValueType, newValEval);
                caster.evaluate(tuple, newVal);
            }

            int max = (int) maxDouble;
            // create list
            IAsterixListBuilder listBuilder;
            if (listType == ATypeTag.ARRAY) {
                if (orderedListBuilder == null) {
                    orderedListBuilder = new OrderedListBuilder();
                }
                listBuilder = orderedListBuilder;
            } else {
                if (unorderedListBuilder == null) {
                    unorderedListBuilder = new UnorderedListBuilder();
                }
                listBuilder = unorderedListBuilder;
            }

            listBuilder.reset((AbstractCollectionType) DefaultOpenFieldType.getDefaultOpenFieldType(listType));
            listAccessor.reset(list.getByteArray(), list.getStartOffset());
            try {
                int counter = 0;
                byte[] targetBytes = target.getByteArray();
                int offset = target.getStartOffset();
                int length = target.getLength();
                for (int i = 0; i < listAccessor.size(); i++) {
                    listAccessor.getOrWriteItem(i, item, storage);
                    if (counter != max && comp.compare(item.getByteArray(), item.getStartOffset(), item.getLength(),
                            targetBytes, offset, length) == 0) {
                        listBuilder.addItem(newVal);
                        counter++;
                    } else {
                        listBuilder.addItem(item);
                    }
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
