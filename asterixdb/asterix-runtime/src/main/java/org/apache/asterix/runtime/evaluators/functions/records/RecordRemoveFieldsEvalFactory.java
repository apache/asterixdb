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
package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class RecordRemoveFieldsEvalFactory implements IScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    private IScalarEvaluatorFactory inputRecordEvalFactory;
    private IScalarEvaluatorFactory removeFieldPathsFactory;
    private ARecordType requiredRecType;
    private ARecordType inputRecType;
    private AOrderedListType inputListType;
    private final SourceLocation sourceLoc;

    public RecordRemoveFieldsEvalFactory(IScalarEvaluatorFactory inputRecordEvalFactory,
            IScalarEvaluatorFactory removeFieldPathsFactory, ARecordType requiredRecType, ARecordType inputRecType,
            AOrderedListType inputListType, SourceLocation sourceLoc) {
        this.inputRecordEvalFactory = inputRecordEvalFactory;
        this.removeFieldPathsFactory = removeFieldPathsFactory;
        this.requiredRecType = requiredRecType;
        this.inputRecType = inputRecType;
        this.inputListType = inputListType;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

        final PointableAllocator pa = new PointableAllocator();
        final IVisitablePointable vp0 = pa.allocateRecordValue(inputRecType);
        final IVisitablePointable vp1 = pa.allocateListValue(inputListType);
        final IPointable inputArg0 = new VoidPointable();
        final IPointable inputArg1 = new VoidPointable();
        final IScalarEvaluator eval0 = inputRecordEvalFactory.createScalarEvaluator(ctx);
        final IScalarEvaluator eval1 = removeFieldPathsFactory.createScalarEvaluator(ctx);
        final IBinaryComparator stringBinaryComparator =
                UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        return new IScalarEvaluator() {
            private final RuntimeRecordTypeInfo runtimeRecordTypeInfo = new RuntimeRecordTypeInfo();

            private final List<RecordBuilder> rbStack = new ArrayList<>();
            private final ArrayBackedValueStorage tabvs = new ArrayBackedValueStorage();
            private final Deque<IVisitablePointable> recordPath = new ArrayDeque<>();

            private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private DataOutput out = resultStorage.getDataOutput();

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                resultStorage.reset();
                eval0.evaluate(tuple, inputArg0);
                eval1.evaluate(tuple, inputArg1);

                if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                    return;
                }

                byte inputTypeTag0 = inputArg0.getByteArray()[inputArg0.getStartOffset()];
                if (inputTypeTag0 != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                    throw new TypeMismatchException(sourceLoc, BuiltinFunctions.REMOVE_FIELDS, 0, inputTypeTag0,
                            ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                }

                byte inputTypeTag1 = inputArg1.getByteArray()[inputArg1.getStartOffset()];
                if (inputTypeTag1 != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                    throw new TypeMismatchException(sourceLoc, BuiltinFunctions.REMOVE_FIELDS, 1, inputTypeTag1,
                            ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
                }

                vp0.set(inputArg0);
                vp1.set(inputArg1);

                ARecordVisitablePointable recordPointable = (ARecordVisitablePointable) vp0;
                AListVisitablePointable listPointable = (AListVisitablePointable) vp1;

                try {
                    recordPath.clear();
                    rbStack.clear();
                    processRecord(requiredRecType, recordPointable, listPointable, 0);
                    rbStack.get(0).write(out, true);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
                result.set(resultStorage);
            }

            private void processRecord(ARecordType requiredType, ARecordVisitablePointable srp,
                    AListVisitablePointable inputList, int nestedLevel) throws IOException {
                if (rbStack.size() < (nestedLevel + 1)) {
                    rbStack.add(new RecordBuilder());
                }

                rbStack.get(nestedLevel).reset(requiredType);
                rbStack.get(nestedLevel).init();

                List<IVisitablePointable> fieldNames = srp.getFieldNames();
                List<IVisitablePointable> fieldValues = srp.getFieldValues();
                List<IVisitablePointable> fieldTypes = srp.getFieldTypeTags();

                for (int i = 0; i < fieldNames.size(); i++) {
                    IVisitablePointable subRecFieldName = fieldNames.get(i);
                    recordPath.push(subRecFieldName);
                    if (isValidPath(inputList)) {
                        if (requiredType != null && requiredType.getTypeTag() != ATypeTag.ANY) {
                            addKeptFieldToSubRecord(requiredType, subRecFieldName, fieldValues.get(i),
                                    fieldTypes.get(i), inputList, nestedLevel);
                        } else {
                            addKeptFieldToSubRecord(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, subRecFieldName,
                                    fieldValues.get(i), fieldTypes.get(i), inputList, nestedLevel);
                        }
                    }
                    recordPath.pop();
                }
            }

            private void addKeptFieldToSubRecord(ARecordType requiredType, IVisitablePointable fieldNamePointable,
                    IVisitablePointable fieldValuePointable, IVisitablePointable fieldTypePointable,
                    AListVisitablePointable inputList, int nestedLevel) throws IOException {

                runtimeRecordTypeInfo.reset(requiredType);
                int pos = runtimeRecordTypeInfo.getFieldIndex(fieldNamePointable.getByteArray(),
                        fieldNamePointable.getStartOffset() + 1, fieldNamePointable.getLength() - 1);
                if (pos >= 0) { // Closed field
                    if (PointableHelper.sameType(ATypeTag.OBJECT, fieldTypePointable)) {
                        processRecord((ARecordType) requiredType.getFieldTypes()[pos],
                                (ARecordVisitablePointable) fieldValuePointable, inputList, nestedLevel + 1);
                        tabvs.reset();
                        rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                        rbStack.get(nestedLevel).addField(pos, tabvs);
                    } else {
                        rbStack.get(nestedLevel).addField(pos, fieldValuePointable);
                    }
                } else { // Open field
                    if (PointableHelper.sameType(ATypeTag.OBJECT, fieldTypePointable)) {
                        processRecord(null, (ARecordVisitablePointable) fieldValuePointable, inputList,
                                nestedLevel + 1);
                        tabvs.reset();
                        rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                        rbStack.get(nestedLevel).addField(fieldNamePointable, tabvs);
                    } else {
                        rbStack.get(nestedLevel).addField(fieldNamePointable, fieldValuePointable);
                    }
                }
            }

            private boolean isValidPath(AListVisitablePointable inputList) throws HyracksDataException {
                List<IVisitablePointable> items = inputList.getItems();
                List<IVisitablePointable> typeTags = inputList.getItemTags();

                int pathLen = recordPath.size();
                for (int i = 0; i < items.size(); i++) {
                    IVisitablePointable item = items.get(i);
                    if (PointableHelper.sameType(ATypeTag.ARRAY, typeTags.get(i))) {
                        List<IVisitablePointable> inputPathItems = ((AListVisitablePointable) item).getItems();

                        if (pathLen == inputPathItems.size()) {
                            boolean match = true;
                            Iterator<IVisitablePointable> fpi = recordPath.iterator();
                            for (int j = inputPathItems.size() - 1; j >= 0; j--) {
                                match &= PointableHelper.isEqual(inputPathItems.get(j), fpi.next(),
                                        stringBinaryComparator);
                                if (!match) {
                                    break;
                                }
                            }
                            if (match) {
                                return false; // Not a valid path for the output record
                            }
                        }
                    } else {
                        if (PointableHelper.isEqual(recordPath.getFirst(), item, stringBinaryComparator)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        };
    }
}
