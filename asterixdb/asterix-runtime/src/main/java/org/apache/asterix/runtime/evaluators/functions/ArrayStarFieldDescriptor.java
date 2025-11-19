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
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class ArrayStarFieldDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayStarFieldDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    private IAType inputListType;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_STAR_FIELD;
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
                return new ArrayStarEval(args, ctx);
            }
        };
    }

    public class ArrayStarEval implements IScalarEvaluator {
        private final IBinaryHashFunction fieldNameHashFunction =
                BinaryHashFunctionFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryHashFunction();
        private final IBinaryComparator fieldNameComparator =
                BinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();
        private final ArrayBackedValueStorage storage;
        private final IScalarEvaluator listEval;
        private final IPointable tempList;
        private final IPointable object;
        private final IPointable castedObject;
        private final ListAccessor listAccessor;
        private final IAsterixListBuilder listBuilder;
        private final IPointable fieldName;
        private final IPointable fieldVal;
        private final IScalarEvaluator fieldNameEval;
        private final TypeCaster caster;
        private final IAType itemType;
        private final boolean openList;
        private boolean addedValue;

        public ArrayStarEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            castedObject = new VoidPointable();
            object = new VoidPointable();
            tempList = new VoidPointable();
            listEval = args[0].createScalarEvaluator(ctx);
            fieldNameEval = args[1].createScalarEvaluator(ctx);
            caster = new TypeCaster(sourceLoc);
            listAccessor = new ListAccessor();
            listBuilder = new OrderedListBuilder();
            fieldName = new VoidPointable();
            fieldVal = new VoidPointable();
            if (inputListType.getTypeTag() == ATypeTag.ARRAY) {
                AOrderedListType listType = (AOrderedListType) inputListType;
                if (listType.isTyped()) {
                    itemType = listType.getItemType();
                    openList = itemType.getTypeTag() == ATypeTag.ANY;
                } else {
                    itemType = null;
                    openList = true;
                }
            } else {
                itemType = null;
                openList = true; // should be ANY type; if the arg is found to be ARRAY at runtime, then it must be open
            }
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            listEval.evaluate(tuple, tempList);
            fieldNameEval.evaluate(tuple, fieldName);
            if (PointableHelper.checkAndSetMissingOrNull(result, tempList, fieldName)) {
                return;
            }
            ATypeTag listTag = ATYPETAGDESERIALIZER.deserialize(tempList.getByteArray()[tempList.getStartOffset()]);
            ATypeTag fieldTag = ATYPETAGDESERIALIZER.deserialize(fieldName.getByteArray()[fieldName.getStartOffset()]);
            if (listTag != ATypeTag.ARRAY || fieldTag != ATypeTag.STRING) {
                PointableHelper.setNull(result);
                return;
            }

            addedValue = false;
            storage.reset();
            listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
            try {
                listAccessor.reset(tempList.getByteArray(), tempList.getStartOffset());
                if (openList) {
                    processOpenList();
                } else {
                    processTypedList();
                }
                if (!addedValue) {
                    PointableHelper.setMissing(result);
                    return;
                }

                storage.reset();
                listBuilder.write(storage.getDataOutput(), true);
                result.set(storage);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        private void processOpenList() throws IOException {
            int numObjects = listAccessor.size();
            for (int objectIndex = 0; objectIndex < numObjects; objectIndex++) {
                listAccessor.getOrWriteItem(objectIndex, object, storage);
                processOpenObject(object);
            }
        }

        private void processTypedList() throws IOException {
            int numObjects = listAccessor.size();
            for (int objectIndex = 0; objectIndex < numObjects; objectIndex++) {
                listAccessor.getOrWriteItem(objectIndex, object, storage);
                processTypedObject(object);
            }
        }

        private void processOpenObject(IPointable object) throws HyracksDataException {
            int serRecordOffset = object.getStartOffset();
            byte[] serRecord = object.getByteArray();
            int serRecordLen = object.getLength();
            if (serRecord[serRecordOffset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                listBuilder.addItem(PointableHelper.NULL_REF);
                return;
            }
            addValue(serRecord, serRecordOffset, serRecordLen);
        }

        private void processTypedObject(IPointable object) throws HyracksDataException {
            int serRecordOffset = object.getStartOffset();
            byte[] serRecord = object.getByteArray();
            if (serRecord[serRecordOffset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                listBuilder.addItem(PointableHelper.NULL_REF);
                return;
            }
            try {
                caster.allocateAndCast(object, itemType, castedObject, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
                serRecordOffset = castedObject.getStartOffset();
                serRecord = castedObject.getByteArray();
                addValue(serRecord, serRecordOffset, castedObject.getLength());
            } finally {
                caster.deallocatePointables();
            }
        }

        private void addValue(byte[] serRecord, int serRecordOffset, int serRecordLen) throws HyracksDataException {
            byte[] serFldName = fieldName.getByteArray();
            int serFldNameOffset = fieldName.getStartOffset();
            int subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetByName(serRecord, serRecordOffset,
                    serRecordLen, serFldName, serFldNameOffset, fieldNameHashFunction, fieldNameComparator);
            if (subFieldOffset < 0) {
                listBuilder.addItem(PointableHelper.NULL_REF);
                return;
            }
            ATypeTag fieldValueTypeTag = ATYPETAGDESERIALIZER.deserialize(serRecord[subFieldOffset]);
            int subFieldLength =
                    NonTaggedFormatUtil.getFieldValueLength(serRecord, subFieldOffset, fieldValueTypeTag, true) + 1;
            fieldVal.set(serRecord, subFieldOffset, subFieldLength);
            listBuilder.addItem(fieldVal);
            addedValue = true;
        }
    }
}
