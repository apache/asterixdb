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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Stack;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.RecordMergeTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

//The record merge evaluator is used to combine two records with no matching fieldnames
//If both records have the same fieldname for a non-record field anywhere in the schema, the merge will fail
//This function is performed on a recursive level, meaning that nested records can be combined
//for instance if both records have a nested field called "metadata"
//where metadata from A is {"comments":"this rocks"}
//and metadata from B is {"index":7, "priority":5}
//Records A and B can be combined yielding a nested record called "metadata"
//That will have all three fields
public class RecordMergeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordMergeDescriptor();
        }
    };

    private ARecordType outRecType;
    private ARecordType inRecType0;
    private ARecordType inRecType1;

    public void reset(IAType outType, IAType inType0, IAType inType1) {
        outRecType = RecordMergeTypeComputer.extractRecordType(outType);
        inRecType0 = RecordMergeTypeComputer.extractRecordType(inType0);
        inRecType1 = RecordMergeTypeComputer.extractRecordType(inType1);
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
            private final AStringSerializerDeserializer aStringSerDer = new AStringSerializerDeserializer();

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final ARecordType recType;
                try {
                    recType = new ARecordType(outRecType.getTypeName(), outRecType.getFieldNames(),
                            outRecType.getFieldTypes(), outRecType.isOpen());
                } catch (AsterixException | HyracksDataException e) {
                    throw new IllegalStateException();
                }

                final PointableAllocator pa = new PointableAllocator();
                final IVisitablePointable vp0 = pa.allocateRecordValue(inRecType0);
                final IVisitablePointable vp1 = pa.allocateRecordValue(inRecType1);

                final ArrayBackedValueStorage abvs0 = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();

                final ICopyEvaluator eval0 = args[0].createEvaluator(abvs0);
                final ICopyEvaluator eval1 = args[1].createEvaluator(abvs1);

                return new ICopyEvaluator() {
                    private final Stack<RecordBuilder> rbStack = new Stack<RecordBuilder>();
                    private final ArrayBackedValueStorage tabvs = new ArrayBackedValueStorage();
                    private final ByteArrayAccessibleOutputStream nameOutputStream = new ByteArrayAccessibleOutputStream();
                    private final ByteArrayInputStream namebais = new ByteArrayInputStream(
                            nameOutputStream.getByteArray());
                    private final DataInputStream namedis = new DataInputStream(namebais);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        abvs0.reset();
                        abvs1.reset();

                        eval0.evaluate(tuple);
                        eval1.evaluate(tuple);

                        if (abvs0.getByteArray()[0] == SER_NULL_TYPE_TAG
                                || abvs1.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                            try {
                                nullSerDe.serialize(ANull.NULL, output.getDataOutput());
                            } catch (HyracksDataException e) {
                                throw new AlgebricksException(e);
                            }
                            return;
                        }

                        vp0.set(abvs0);
                        vp1.set(abvs1);

                        ARecordVisitablePointable rp0 = (ARecordVisitablePointable) vp0;
                        ARecordVisitablePointable rp1 = (ARecordVisitablePointable) vp1;

                        try {
                            mergeFields(recType, rp0, rp1, true, 0);

                            rbStack.get(0).write(output.getDataOutput(), true);
                        } catch (IOException | AsterixException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    private void mergeFields(ARecordType combinedType, ARecordVisitablePointable leftRecord,
                            ARecordVisitablePointable rightRecord, boolean openFromParent, int nestedLevel)
                                    throws IOException, AsterixException, AlgebricksException {
                        if (rbStack.size() < (nestedLevel + 1)) {
                            rbStack.push(new RecordBuilder());
                        }

                        rbStack.get(nestedLevel).reset(combinedType);
                        rbStack.get(nestedLevel).init();
                        //Add all fields from left record
                        for (int i = 0; i < leftRecord.getFieldNames().size(); i++) {
                            IVisitablePointable leftName = leftRecord.getFieldNames().get(i);
                            IVisitablePointable leftValue = leftRecord.getFieldValues().get(i);
                            boolean foundMatch = false;
                            for (int j = 0; j < rightRecord.getFieldNames().size(); j++) {
                                IVisitablePointable rightName = rightRecord.getFieldNames().get(j);
                                IVisitablePointable rightValue = rightRecord.getFieldValues().get(j);
                                if (rightName.equals(leftName)) {
                                    //Field was found on the right. Merge Sub Records
                                    if (rightValue.getByteArray()[0] != ATypeTag.RECORD.serialize()
                                            || leftValue.getByteArray()[0] != ATypeTag.RECORD.serialize()) {
                                        //The fields need to be records in order to merge
                                        throw new AlgebricksException("Duplicate field found");
                                    } else {
                                        //We are merging two sub records
                                        addFieldToSubRecord(combinedType, leftName, leftValue, rightValue,
                                                openFromParent, nestedLevel);
                                    }
                                    foundMatch = true;
                                }
                            }
                            if (!foundMatch) {
                                addFieldToSubRecord(combinedType, leftName, leftValue, null, openFromParent,
                                        nestedLevel);
                            }
                        }
                        //Repeat for right side (ignoring duplicates this time)
                        for (int j = 0; j < rightRecord.getFieldNames().size(); j++) {
                            IVisitablePointable rightName = rightRecord.getFieldNames().get(j);
                            IVisitablePointable rightValue = rightRecord.getFieldValues().get(j);
                            boolean foundMatch = false;
                            for (int i = 0; i < leftRecord.getFieldNames().size(); i++) {
                                IVisitablePointable leftName = leftRecord.getFieldNames().get(i);
                                if (rightName.equals(leftName)) {
                                    foundMatch = true;
                                }
                            }
                            if (!foundMatch) {
                                addFieldToSubRecord(combinedType, rightName, rightValue, null, openFromParent,
                                        nestedLevel);
                            }
                        }
                    }

                    //Takes in a record type, field name, and the field values (which are record) from two records
                    //Merges them into one record of combinedType
                    //And adds that record as a field to the Record in subrb
                    //the second value can be null, indicated that you just add the value of left as a field to subrb
                    private void addFieldToSubRecord(ARecordType combinedType, IVisitablePointable fieldNamePointable,
                            IVisitablePointable leftValue, IVisitablePointable rightValue, boolean openFromParent,
                            int nestedLevel) throws IOException, AsterixException, AlgebricksException {

                        nameOutputStream.reset();
                        nameOutputStream.write(fieldNamePointable.getByteArray(),
                                fieldNamePointable.getStartOffset() + 1, fieldNamePointable.getLength());
                        namedis.reset();
                        String fieldName = aStringSerDer.deserialize(namedis).getStringValue();

                        //Add the merged field
                        if (combinedType.isClosedField(fieldName)) {
                            int pos = combinedType.getFieldIndex(fieldName);
                            if (rightValue == null) {
                                rbStack.get(nestedLevel).addField(pos, leftValue);
                            } else {
                                mergeFields((ARecordType) combinedType.getFieldType(fieldName),
                                        (ARecordVisitablePointable) leftValue, (ARecordVisitablePointable) rightValue,
                                        false, nestedLevel + 1);
                                tabvs.reset();
                                rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                                rbStack.get(nestedLevel).addField(pos, tabvs);
                            }
                        } else {
                            if (rightValue == null) {
                                rbStack.get(nestedLevel).addField(fieldNamePointable, leftValue);
                            } else {
                                mergeFields((ARecordType) combinedType.getFieldType(fieldName),
                                        (ARecordVisitablePointable) leftValue, (ARecordVisitablePointable) rightValue,
                                        false, nestedLevel + 1);
                                tabvs.reset();
                                rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                                rbStack.get(nestedLevel).addField(fieldNamePointable, tabvs);
                            }
                        }
                    }

                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.RECORD_MERGE;
    }
}
