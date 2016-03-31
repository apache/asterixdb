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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FieldAccessByIndexEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory recordEvalFactory;
    private IScalarEvaluatorFactory fieldIndexEvalFactory;
    private int nullBitmapSize;
    private ARecordType recordType;

    public FieldAccessByIndexEvalFactory(IScalarEvaluatorFactory recordEvalFactory,
            IScalarEvaluatorFactory fieldIndexEvalFactory, ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.fieldIndexEvalFactory = fieldIndexEvalFactory;
        this.recordType = recordType;
        this.nullBitmapSize = ARecordType.computeNullBitmapSize(recordType);
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
        return new IScalarEvaluator() {
            private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private DataOutput out = resultStorage.getDataOutput();

            private IPointable inputArg0 = new VoidPointable();
            private IPointable inputArg1 = new VoidPointable();
            private IScalarEvaluator eval0 = recordEvalFactory.createScalarEvaluator(ctx);
            private IScalarEvaluator eval1 = fieldIndexEvalFactory.createScalarEvaluator(ctx);
            @SuppressWarnings("unchecked")
            private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
            private int fieldIndex;
            private int fieldValueOffset;
            private int fieldValueLength;
            private IAType fieldValueType;
            private ATypeTag fieldValueTypeTag = ATypeTag.NULL;

            /*
             * inputArg0: the record
             * inputArg1: the index
             *
             * This method outputs into IHyracksTaskContext context [field type tag (1 byte)][the field data]
             */
            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                try {
                    resultStorage.reset();
                    eval0.evaluate(tuple, inputArg0);
                    byte[] serRecord = inputArg0.getByteArray();
                    int offset = inputArg0.getStartOffset();

                    if (serRecord[offset] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                        nullSerde.serialize(ANull.NULL, out);
                        result.set(resultStorage);
                        return;
                    }

                    if (serRecord[offset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                        throw new AlgebricksException("Field accessor is not defined for values of type "
                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serRecord[offset]));
                    }
                    eval1.evaluate(tuple, inputArg1);
                    fieldIndex = IntegerPointable.getInteger(inputArg1.getByteArray(), inputArg1.getStartOffset() + 1);
                    fieldValueType = recordType.getFieldTypes()[fieldIndex];
                    fieldValueOffset = ARecordSerializerDeserializer.getFieldOffsetById(serRecord, offset, fieldIndex,
                            nullBitmapSize, recordType.isOpen());

                    if (fieldValueOffset == 0) {
                        // the field is null, we checked the null bit map
                        out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                        result.set(resultStorage);
                        return;
                    }

                    if (fieldValueType.getTypeTag().equals(ATypeTag.UNION)) {
                        if (((AUnionType) fieldValueType).isNullableType()) {
                            fieldValueTypeTag = ((AUnionType) fieldValueType).getNullableType().getTypeTag();
                            fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(serRecord, fieldValueOffset,
                                    fieldValueTypeTag, false);
                            out.writeByte(fieldValueTypeTag.serialize());
                        } else {
                            // union .. the general case
                            throw new NotImplementedException();
                        }
                    } else {
                        fieldValueTypeTag = fieldValueType.getTypeTag();
                        fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(serRecord, fieldValueOffset,
                                fieldValueTypeTag, false);
                        out.writeByte(fieldValueTypeTag.serialize());
                    }
                    out.write(serRecord, fieldValueOffset, fieldValueLength);
                    result.set(resultStorage);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }
}
