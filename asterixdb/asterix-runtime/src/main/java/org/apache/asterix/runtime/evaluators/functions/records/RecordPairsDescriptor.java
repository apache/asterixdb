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
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
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

public class RecordPairsDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordPairsDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private ARecordType recType;

    public void reset(ARecordType recType) {
        this.recType = recType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.RECORD_PAIRS;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
                // For writing each individual output record.
                final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
                final DataOutput itemOutput = itemStorage.getDataOutput();
                final RecordBuilder recBuilder = new RecordBuilder();
                recBuilder.reset(ARecordType.FULLY_OPEN_RECORD_TYPE);

                // For writing the resulting list of records.
                final OrderedListBuilder listBuilder = new OrderedListBuilder();
                final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                final DataOutput resultOut = resultStorage.getDataOutput();

                // Sets up the constant field names, "name" for the key field, "value" for the value field.
                final ArrayBackedValueStorage nameStorage = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage valueStorage = new ArrayBackedValueStorage();
                AObjectSerializerDeserializer serde = AObjectSerializerDeserializer.INSTANCE;
                try {
                    serde.serialize(new AString("name"), nameStorage.getDataOutput());
                    serde.serialize(new AString("value"), valueStorage.getDataOutput());
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }

                return new IScalarEvaluator() {
                    private final IScalarEvaluator argEvaluator = args[0].createScalarEvaluator(ctx);
                    private final IPointable argPtr = new VoidPointable();
                    private final ARecordVisitablePointable recordVisitablePointable = new ARecordVisitablePointable(
                            recType);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        try {
                            // Resets the result storage.
                            resultStorage.reset();

                            // Gets the input record.
                            argEvaluator.evaluate(tuple, argPtr);
                            byte inputTypeTag = argPtr.getByteArray()[argPtr.getStartOffset()];
                            if (inputTypeTag != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                                throw new AlgebricksException("Function " + RecordPairsDescriptor.this.getIdentifier()
                                        + " expects a record as the input, " + "but get a "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputTypeTag));
                            }
                            recordVisitablePointable.set(argPtr);

                            listBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
                            List<IVisitablePointable> fieldNames = recordVisitablePointable.getFieldNames();
                            List<IVisitablePointable> fieldValues = recordVisitablePointable.getFieldValues();
                            // Adds each field of the input record as a key-value pair into the result.
                            int numFields = recordVisitablePointable.getFieldNames().size();
                            for (int fieldIndex = 0; fieldIndex < numFields; ++fieldIndex) {
                                itemStorage.reset();
                                recBuilder.init();
                                recBuilder.addField(nameStorage, fieldNames.get(fieldIndex));
                                recBuilder.addField(valueStorage, fieldValues.get(fieldIndex));
                                recBuilder.write(itemOutput, true);
                                listBuilder.addItem(itemStorage);
                            }

                            // Writes the result and sets the result pointable.
                            listBuilder.write(resultOut, true);
                            result.set(resultStorage);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }
}
