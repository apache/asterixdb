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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class RecordRemoveFieldsEvalFactory implements ICopyEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private static final byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private static final byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    private ICopyEvaluatorFactory inputRecordEvalFactory;
    private ICopyEvaluatorFactory removeFieldPathsFactory;
    private ARecordType requiredRecType;
    private ARecordType inputRecType;
    private AOrderedListType inputListType;

    public RecordRemoveFieldsEvalFactory(ICopyEvaluatorFactory inputRecordEvalFactory,
            ICopyEvaluatorFactory removeFieldPathsFactory, ARecordType requiredRecType, ARecordType inputRecType,
            AOrderedListType inputListType) {
        this.inputRecordEvalFactory = inputRecordEvalFactory;
        this.removeFieldPathsFactory = removeFieldPathsFactory;
        this.requiredRecType = requiredRecType;
        this.inputRecType = inputRecType;
        this.inputListType = inputListType;

    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

        final PointableAllocator pa = new PointableAllocator();
        final IVisitablePointable vp0 = pa.allocateRecordValue(inputRecType);
        final IVisitablePointable vp1 = pa.allocateListValue(inputListType);
        final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
        final ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
        final ICopyEvaluator eval0 = inputRecordEvalFactory.createEvaluator(outInput0);
        final ICopyEvaluator eval1 = removeFieldPathsFactory.createEvaluator(outInput1);

        return new ICopyEvaluator() {
            private final RuntimeRecordTypeInfo runtimeRecordTypeInfo = new RuntimeRecordTypeInfo();

            private final List<RecordBuilder> rbStack = new ArrayList<>();
            private final ArrayBackedValueStorage tabvs = new ArrayBackedValueStorage();
            private final Deque<IVisitablePointable> recordPath = new ArrayDeque<>();


            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                outInput1.reset();

                eval0.evaluate(tuple);
                eval1.evaluate(tuple);

                if (outInput0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerDe.serialize(ANull.NULL, output.getDataOutput());
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    return;
                }

                if (outInput0.getByteArray()[0] != SER_RECORD_TYPE_TAG) {
                    throw new AlgebricksException(AsterixBuiltinFunctions.REMOVE_FIELDS.getName()
                            + ": expects input type " + inputRecType + ", but got "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0]));
                }

                if (outInput1.getByteArray()[0] != SER_ORDEREDLIST_TYPE_TAG) {
                    throw new AlgebricksException(AsterixBuiltinFunctions.REMOVE_FIELDS.getName()
                            + ": expects input type " + inputListType + ", but got "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput1.getByteArray()[0]));
                }

                vp0.set(outInput0);
                vp1.set(outInput1);

                ARecordVisitablePointable recordPointable = (ARecordVisitablePointable) vp0;
                AListVisitablePointable listPointable = (AListVisitablePointable) vp1;

                try {
                    recordPath.clear();
                    rbStack.clear();
                    processRecord(requiredRecType, recordPointable, listPointable, 0);
                    rbStack.get(0).write(output.getDataOutput(), true);
                } catch (IOException | AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }

            private void processRecord(ARecordType requiredType, ARecordVisitablePointable srp,
                    AListVisitablePointable inputList, int nestedLevel) throws IOException, AsterixException,
                    AlgebricksException {
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
                    AListVisitablePointable inputList, int nestedLevel) throws IOException, AsterixException,
                    AlgebricksException {

                runtimeRecordTypeInfo.reset(requiredType);
                int pos = runtimeRecordTypeInfo.getFieldIndex(fieldNamePointable.getByteArray(),
                        fieldNamePointable.getStartOffset() + 1, fieldNamePointable.getLength() - 1);
                if (pos >= 0) { // Closed field
                    if (PointableHelper.sameType(ATypeTag.RECORD, fieldTypePointable)) {
                        processRecord((ARecordType) requiredType.getFieldTypes()[pos],
                                (ARecordVisitablePointable) fieldValuePointable, inputList, nestedLevel + 1);
                        tabvs.reset();
                        rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                        rbStack.get(nestedLevel).addField(pos, tabvs);
                    } else {
                        rbStack.get(nestedLevel).addField(pos, fieldValuePointable);
                    }
                } else { // Open field
                    if (PointableHelper.sameType(ATypeTag.RECORD, fieldTypePointable)) {
                        processRecord(null, (ARecordVisitablePointable) fieldValuePointable, inputList, nestedLevel + 1);
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
                    if (PointableHelper.sameType(ATypeTag.ORDEREDLIST, typeTags.get(i))) {
                        List<IVisitablePointable> inputPathItems = ((AListVisitablePointable) item).getItems();

                        if (pathLen == inputPathItems.size()) {
                            boolean match = true;
                            Iterator<IVisitablePointable> fpi = recordPath.iterator();
                            for (int j = inputPathItems.size() - 1; j >= 0; j--) {
                                match &= PointableHelper.isEqual(inputPathItems.get(j), fpi.next());
                                if (!match)
                                    break;
                            }
                            if (match)
                                return false; // Not a valid path for the output record
                        }
                    } else {
                        if (PointableHelper.isEqual(recordPath.getFirst(), item)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        };
    }
}
