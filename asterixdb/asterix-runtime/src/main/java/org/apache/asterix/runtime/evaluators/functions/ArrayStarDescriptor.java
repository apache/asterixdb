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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * <pre>
 * array_star(ordered_list) returns a new object. The input ordered list is supposed to be a list of objects:
 * [{"id":1, "dept":"CS"}, {"id":2, "dept":"FIN"}, {"id":3, "dept":"CS"}]
 * For the returned object, each field has a value = list of values of that specific field taken from each object in the
 * input list.
 *
 * Ex1: array_star([{"a":1, "b":2}, {"a":9, "b":4}]) will produce: {"a":[1, 9], "b":[2, 4]}
 * Ex2: array_star([{"a":1}, {"a":9, "b":4}]) will produce: {"a":[1, 9], "b":[null, 4]}
 * Ex3: array_star([{"a":1, "c":5}, {"a":9, "b":4}]) will produce: {"a":[1, 9], "b":[null, 4], "c":[5,null]}
 * Ex4: array_star([{"c":5, "a":1}, "non_object"]) will produce: {"a":[1, null], "c":[5,null]}
 * Ex5: array_star(["non_object1", "non_object2"]) will produce: {} (i.e., missing)
 *
 * Note that in the final object result, the fields are ordered by their names regardless of their original order in the
 * object items in the input list. "a" comes before "c". However, for every field, all the items in each list must not
 * be ordered. They should appear in the sequence they appear in the input list.
 * For Ex1, "a":[1,9] in the final result, item at index 0 comes from object at index 0 (which is 1).
 *
 * It throws an error at compile time if the number of arguments != 1
 *
 * It returns in order:
 * 1. missing, if any argument is missing.
 * 2. null, if the list arg is null or it's not an ordered list.
 * 3. missing, if input list is missing the notion of fields.
 *    E.g., the input list contains no object items (e.g., list of int), or all objects have no fields.
 * 4. otherwise, a new object.
 *
 * </pre>
 */

@MissingNullInOutFunction
public class ArrayStarDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayStarDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    private IAType inputListType;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_STAR;
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

    public class UTF8StringComparator implements Comparator<IValueReference> {
        private final IBinaryComparator comp = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        @Override
        public int compare(IValueReference val1, IValueReference val2) {
            try {
                return PointableHelper.compareStringBinValues(val1, val2, comp);
            } catch (HyracksDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    protected class FieldNameToValues implements IValueReference {
        private IVisitablePointable fieldName;
        private List<IVisitablePointable> values;

        @Override
        public byte[] getByteArray() {
            return fieldName.getByteArray();
        }

        @Override
        public int getStartOffset() {
            return fieldName.getStartOffset();
        }

        @Override
        public int getLength() {
            return fieldName.getLength();
        }
    }

    protected class FieldNameToValuesAllocator implements IObjectFactory<FieldNameToValues, ATypeTag> {

        @Override
        public FieldNameToValues create(ATypeTag arg) {
            return new FieldNameToValues();
        }
    }

    public class ArrayStarEval implements IScalarEvaluator {
        private final IBinaryComparator binaryStrComp =
                UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        private final UTF8StringComparator comp = new UTF8StringComparator();
        private final ArrayBackedValueStorage storage;
        private final IScalarEvaluator listEval;
        private final IPointable list;
        private final IPointable tempList;
        private final IPointable object;
        private final CastTypeEvaluator caster;
        private final ListAccessor listAccessor;
        private final RecordBuilder recordBuilder;
        private final IAsterixListBuilder listBuilder;
        private final PointableAllocator pointableAllocator;
        private final List<FieldNameToValues> fieldNameToValuesList;
        private final PriorityQueue<FieldNameToValues> tempMinHeap;
        private final IObjectPool<List<IVisitablePointable>, ATypeTag> arrayListAllocator;
        private final IObjectPool<FieldNameToValues, ATypeTag> fieldNameToValuesAllocator;

        public ArrayStarEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            object = new VoidPointable();
            list = new VoidPointable();
            tempList = new VoidPointable();
            listEval = args[0].createScalarEvaluator(ctx);
            caster = new CastTypeEvaluator();
            listAccessor = new ListAccessor();
            recordBuilder = new RecordBuilder();
            listBuilder = new OrderedListBuilder();
            pointableAllocator = new PointableAllocator();
            fieldNameToValuesList = new ArrayList<>();
            tempMinHeap = new PriorityQueue<>(comp);
            arrayListAllocator = new ListObjectPool<>(new ArrayListFactory<>());
            fieldNameToValuesAllocator = new ListObjectPool<>(new FieldNameToValuesAllocator());
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            storage.reset();
            listEval.evaluate(tuple, tempList);

            if (PointableHelper.checkAndSetMissingOrNull(result, tempList)) {
                return;
            }

            ATypeTag listTag = ATYPETAGDESERIALIZER.deserialize(tempList.getByteArray()[tempList.getStartOffset()]);
            if (listTag != ATypeTag.ARRAY) {
                PointableHelper.setNull(result);
                return;
            }

            try {
                caster.resetAndAllocate(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE, inputListType, listEval);
                caster.cast(tempList, list);

                tempMinHeap.clear();
                fieldNameToValuesList.clear();
                listAccessor.reset(list.getByteArray(), list.getStartOffset());
                int numObjects = listAccessor.size();

                for (int objectIndex = 0; objectIndex < numObjects; objectIndex++) {
                    listAccessor.getOrWriteItem(objectIndex, object, storage);
                    processObject(object, objectIndex, numObjects);
                }

                if (fieldNameToValuesList.isEmpty()) {
                    PointableHelper.setMissing(result);
                    return;
                }
                for (int i = 0; i < fieldNameToValuesList.size(); i++) {
                    tempMinHeap.add(fieldNameToValuesList.get(i));
                }
                recordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
                recordBuilder.init();

                FieldNameToValues fieldNameToValues;
                IVisitablePointable oneValue;
                while (!tempMinHeap.isEmpty()) {
                    fieldNameToValues = tempMinHeap.poll();
                    listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
                    for (int k = 0; k < fieldNameToValues.values.size(); k++) {
                        oneValue = fieldNameToValues.values.get(k);
                        if (oneValue == null) {
                            listBuilder.addItem(PointableHelper.NULL_REF);
                        } else {
                            listBuilder.addItem(oneValue);
                        }
                    }
                    storage.reset();
                    listBuilder.write(storage.getDataOutput(), true);
                    recordBuilder.addField(fieldNameToValues.fieldName, storage);
                }

                storage.reset();
                recordBuilder.write(storage.getDataOutput(), true);
                result.set(storage);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            } finally {
                pointableAllocator.reset();
                arrayListAllocator.reset();
                fieldNameToValuesAllocator.reset();
                caster.deallocatePointables();
            }
        }

        private void processObject(IPointable object, int objectIndex, int numObjects) throws HyracksDataException {
            ARecordVisitablePointable record;
            // process only objects (records)
            if (object.getByteArray()[object.getStartOffset()] == ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                record = pointableAllocator.allocateRecordValue(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
                record.set(object.getByteArray(), object.getStartOffset(), object.getLength());

                List<IVisitablePointable> fieldNames = record.getFieldNames();
                List<IVisitablePointable> fieldValues = record.getFieldValues();
                List<IVisitablePointable> values;
                IVisitablePointable fieldName;
                for (int j = 0; j < fieldNames.size(); j++) {
                    fieldName = fieldNames.get(j);
                    FieldNameToValues fieldNameToValues = findField(fieldName, fieldNameToValuesList, binaryStrComp);

                    if (fieldNameToValues == null) {
                        // new field name
                        fieldNameToValues = fieldNameToValuesAllocator.allocate(null);
                        values = arrayListAllocator.allocate(null);
                        clear(values, numObjects);
                        fieldNameToValues.fieldName = fieldName;
                        fieldNameToValues.values = values;
                        fieldNameToValuesList.add(fieldNameToValues);
                    } else {
                        // field name already exists, get the values vector
                        values = fieldNameToValues.values;
                    }
                    values.set(objectIndex, fieldValues.get(j));
                }
            }
        }

        private void clear(List<IVisitablePointable> values, int numObjects) {
            values.clear();
            for (int i = 1; i <= numObjects; i++) {
                values.add(null);
            }
        }

        private FieldNameToValues findField(IVisitablePointable fieldName, List<FieldNameToValues> fieldNamesList,
                IBinaryComparator strComp) throws HyracksDataException {
            FieldNameToValues anotherFieldName;
            for (int i = 0; i < fieldNamesList.size(); i++) {
                anotherFieldName = fieldNamesList.get(i);
                if (PointableHelper.isEqual(fieldName, anotherFieldName.fieldName, strComp)) {
                    return anotherFieldName;
                }
            }
            return null;
        }
    }
}
