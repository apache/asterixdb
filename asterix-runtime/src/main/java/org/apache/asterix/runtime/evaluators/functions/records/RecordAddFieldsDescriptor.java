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
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.comparators.ListItemBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.hash.ListItemBinaryHashFunctionFactory;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.TypeComputerUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.BinaryHashMap;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class RecordAddFieldsDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordAddFieldsDescriptor();
        }
    };
    private static final long serialVersionUID = 1L;
    private ARecordType outRecType;
    private ARecordType inRecType;
    private AOrderedListType inListType;
    private IAType inputFieldListItemType;

    public void reset(IAType outType, IAType inType0, IAType inType1) {
        outRecType = TypeComputerUtils.extractRecordType(outType);
        inRecType = TypeComputerUtils.extractRecordType(inType0);
        inListType = TypeComputerUtils.extractOrderedListType(inType1);
        inputFieldListItemType = inListType.getItemType();
        if (inputFieldListItemType == null || inputFieldListItemType.getTypeTag() == ATypeTag.ANY) {
            inputFieldListItemType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
        }
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;
            private final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
            private final byte SER_ORDERED_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
            private final byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final PointableAllocator allocator = new PointableAllocator();
                final IVisitablePointable vp0 = allocator.allocateRecordValue(inRecType);
                final IVisitablePointable vp1 = allocator.allocateListValue(inListType);

                final ArrayBackedValueStorage abvs0 = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();

                final ICopyEvaluator eval0 = args[0].createEvaluator(abvs0);
                final ICopyEvaluator eval1 = args[1].createEvaluator(abvs1);

                final ArrayBackedValueStorage fieldNamePointable = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage fieldValuePointer = new ArrayBackedValueStorage();
                final PointableHelper pointableHelper = new PointableHelper();
                try {
                    pointableHelper.serializeString("field-name", fieldNamePointable, true);
                    pointableHelper.serializeString("field-value", fieldValuePointer, true);
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }

                return new ICopyEvaluator() {
                    public static final int TABLE_FRAME_SIZE = 32768; // the default 32k frame size
                    public static final int TABLE_SIZE = 100; // the default 32k frame size
                    private final RecordBuilder recordBuilder = new RecordBuilder();
                    private final RuntimeRecordTypeInfo requiredRecordTypeInfo = new RuntimeRecordTypeInfo();

                    private final IBinaryHashFunction putHashFunc = ListItemBinaryHashFunctionFactory.INSTANCE
                            .createBinaryHashFunction();
                    private final IBinaryHashFunction getHashFunc = ListItemBinaryHashFunctionFactory.INSTANCE
                            .createBinaryHashFunction();
                    private final BinaryHashMap.BinaryEntry keyEntry = new BinaryHashMap.BinaryEntry();
                    private final BinaryHashMap.BinaryEntry valEntry = new BinaryHashMap.BinaryEntry();
                    private final IVisitablePointable tempValReference = allocator.allocateEmpty();
                    private final IBinaryComparator cmp = ListItemBinaryComparatorFactory.INSTANCE
                            .createBinaryComparator();
                    private BinaryHashMap hashMap = new BinaryHashMap(TABLE_SIZE, TABLE_FRAME_SIZE, putHashFunc,
                            getHashFunc, cmp);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        recordBuilder.reset(outRecType);
                        requiredRecordTypeInfo.reset(outRecType);
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

                        // Make sure we get a valid record
                        if (abvs0.getByteArray()[0] != SER_RECORD_TYPE_TAG) {
                            throw new AlgebricksException("Expected an ordederlist of type " + inRecType + " but "
                                    + "got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(abvs0.getByteArray()[0]));
                        }

                        // Make sure we get a valid list
                        if (abvs1.getByteArray()[0] != SER_ORDERED_TYPE_TAG) {
                            throw new AlgebricksException("Expected an ordederlist of type " + inListType + " but "
                                    + "got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(abvs1.getByteArray()[0]));
                        }

                        vp0.set(abvs0);
                        vp1.set(abvs1);

                        try {
                            ARecordVisitablePointable recordPointable = (ARecordVisitablePointable) vp0;
                            AListVisitablePointable listPointable = (AListVisitablePointable) vp1;

                            // Initialize our hashmap
                            int tableSize = recordPointable.getFieldNames().size() + listPointable.getItems().size();
                            // Construct a new hash table only if table size is larger than the default
                            // Thus avoiding unnecessary object construction
                            if (hashMap == null || tableSize > TABLE_SIZE) {
                                hashMap = new BinaryHashMap(tableSize, TABLE_FRAME_SIZE, putHashFunc, getHashFunc, cmp);
                            } else {
                                hashMap.clear();
                            }
                            addFields(recordPointable, listPointable);
                            recordBuilder.write(output.getDataOutput(), true);
                        } catch (IOException | AsterixException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    private void addFields(ARecordVisitablePointable inputRecordPointer,
                            AListVisitablePointable listPointable) throws AlgebricksException {
                        List<IVisitablePointable> inputRecordFieldNames = inputRecordPointer.getFieldNames();
                        List<IVisitablePointable> inputRecordFieldValues = inputRecordPointer.getFieldValues();
                        List<IVisitablePointable> inputFields = listPointable.getItems();
                        IVisitablePointable namePointable = null;
                        IVisitablePointable valuePointable = null;
                        int numInputRecordFields = inputRecordFieldNames.size();

                        try {
                            // Add original record without duplicate checking
                            for (int i = 0; i < numInputRecordFields; ++i) {
                                IVisitablePointable fnp = inputRecordFieldNames.get(i);
                                IVisitablePointable fvp = inputRecordFieldValues.get(i);
                                int pos = requiredRecordTypeInfo.getFieldIndex(fnp.getByteArray(),
                                        fnp.getStartOffset() + 1, fnp.getLength() - 1);
                                if (pos >= 0) {
                                    recordBuilder.addField(pos, fvp);
                                } else {
                                    recordBuilder.addField(fnp, fvp);
                                }
                                keyEntry.set(fnp.getByteArray(), fnp.getStartOffset(), fnp.getLength());
                                valEntry.set(fvp.getByteArray(), fvp.getStartOffset(), fvp.getLength());
                                hashMap.put(keyEntry, valEntry);
                            }

                            // Get the fields from a list of records
                            for (int i = 0; i < inputFields.size(); i++) {
                                if (!PointableHelper.sameType(ATypeTag.RECORD, inputFields.get(i))) {
                                    throw new AsterixException("Expected list of record, got "
                                            + PointableHelper.getTypeTag(inputFields.get(i)));
                                }
                                List<IVisitablePointable> names = ((ARecordVisitablePointable) inputFields.get(i))
                                        .getFieldNames();
                                List<IVisitablePointable> values = ((ARecordVisitablePointable) inputFields.get(i))
                                        .getFieldValues();

                                // Get name and value of the field to be added
                                // Use loop to account for the cases where users switches the order of the fields
                                IVisitablePointable fieldName;
                                for (int j = 0; j < names.size(); j++) {
                                    fieldName = names.get(j);
                                    // if fieldName is "field-name" then read the name
                                    if (PointableHelper.byteArrayEqual(fieldNamePointable, fieldName)) {
                                        namePointable = values.get(j);
                                    } else { // otherwise the fieldName is "field-value". Thus, read the value
                                        valuePointable = values.get(j);
                                    }
                                }

                                if (namePointable == null || valuePointable == null) {
                                    throw new AlgebricksException("Trying to add a null field name or field value");
                                }

                                // Check that the field being added is a valid field
                                int pos = requiredRecordTypeInfo.getFieldIndex(namePointable.getByteArray(),
                                        namePointable.getStartOffset() + 1, namePointable.getLength() - 1);

                                keyEntry.set(namePointable.getByteArray(), namePointable.getStartOffset(),
                                        namePointable.getLength());
                                // Check if already in our built record
                                BinaryHashMap.BinaryEntry entry = hashMap.get(keyEntry);
                                if (entry != null) {
                                    tempValReference.set(entry.buf, entry.off, entry.len);
                                    // If value is not equal throw conflicting duplicate field, otherwise ignore
                                    if (!PointableHelper.byteArrayEqual(valuePointable, tempValReference)) {
                                        throw new AlgebricksException("Conflicting duplicate field found.");
                                    }
                                } else {
                                    if (pos > -1) {
                                        recordBuilder.addField(pos, valuePointable);
                                    } else {
                                        recordBuilder.addField(namePointable, valuePointable);
                                    }
                                    valEntry.set(valuePointable.getByteArray(), valuePointable.getStartOffset(),
                                            valuePointable.getLength());
                                    hashMap.put(keyEntry, valEntry);
                                }
                            }
                        } catch (AsterixException | HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.ADD_FIELDS;
    }
}
