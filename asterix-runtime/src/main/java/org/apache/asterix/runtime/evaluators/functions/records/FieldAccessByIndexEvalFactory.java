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
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FieldAccessByIndexEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ICopyEvaluatorFactory fieldIndexEvalFactory;
    private int nullBitmapSize;
    private ARecordType recordType;
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

    public FieldAccessByIndexEvalFactory(ICopyEvaluatorFactory recordEvalFactory,
            ICopyEvaluatorFactory fieldIndexEvalFactory, ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.fieldIndexEvalFactory = fieldIndexEvalFactory;
        this.recordType = recordType;
        this.nullBitmapSize = ARecordType.computeNullBitmapSize(recordType);

    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {

            private DataOutput out = output.getDataOutput();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private ICopyEvaluator eval1 = fieldIndexEvalFactory.createEvaluator(outInput1);
            @SuppressWarnings("unchecked")
            private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
            private int fieldIndex;
            private int fieldValueOffset;
            private int fieldValueLength;
            private IAType fieldValueType;
            private ATypeTag fieldValueTypeTag = ATypeTag.NULL;

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                try {
                    outInput0.reset();
                    eval0.evaluate(tuple);
                    outInput1.reset();
                    eval1.evaluate(tuple);
                    byte[] serRecord = outInput0.getByteArray();

                    if (serRecord[0] == SER_NULL_TYPE_TAG) {
                        nullSerde.serialize(ANull.NULL, out);
                        return;
                    }

                    if (serRecord[0] != SER_RECORD_TYPE_TAG) {
                        throw new AlgebricksException("Field accessor is not defined for values of type "
                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serRecord[0]));
                    }

                    fieldIndex = IntegerPointable.getInteger(outInput1.getByteArray(), 1);
                    fieldValueOffset = ARecordSerializerDeserializer.getFieldOffsetById(serRecord, fieldIndex,
                            nullBitmapSize, recordType.isOpen());

                    if (fieldValueOffset == 0) {
                        // the field is null, we checked the null bit map
                        out.writeByte(SER_NULL_TYPE_TAG);
                        return;
                    }

                    fieldValueType = recordType.getFieldTypes()[fieldIndex];
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

                } catch (IOException e) {
                    throw new AlgebricksException(e);
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }

}
