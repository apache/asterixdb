/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FieldAccessNestedEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ICopyEvaluatorFactory fldNameEvalFactory;
    private ARecordType recordType;
    private List<String> fieldPath;

    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

    public FieldAccessNestedEvalFactory(ICopyEvaluatorFactory recordEvalFactory,
            ICopyEvaluatorFactory fldNameEvalFactory, ARecordType recordType, List<String> fldName) {
        this.recordEvalFactory = recordEvalFactory;
        this.fldNameEvalFactory = fldNameEvalFactory;
        this.recordType = recordType;
        this.fieldPath = fldName;

    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {

            private DataOutput out = output.getDataOutput();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private ByteArrayAccessibleOutputStream subRecordTmpStream = new ByteArrayAccessibleOutputStream();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private ICopyEvaluator eval1 = fldNameEvalFactory.createEvaluator(outInput1);
            @SuppressWarnings("unchecked")
            private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
            private ArrayBackedValueStorage[] abvs = new ArrayBackedValueStorage[fieldPath.size()];
            private DataOutput[] dos = new DataOutput[fieldPath.size()];
            private AString[] as = new AString[fieldPath.size()];

            {
                for (int i = 0; i < fieldPath.size(); i++) {
                    abvs[i] = new ArrayBackedValueStorage();
                    dos[i] = abvs[i].getDataOutput();
                    as[i] = new AString(fieldPath.get(i));
                    try {
                        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as[i].getType())
                                .serialize(as[i], dos[i]);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                }
                recordType = recordType.deepCopy(recordType);

            }

            public int checkType(byte[] serRecord) throws AlgebricksException {
                if (serRecord[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    return -1;
                }

                if (serRecord[0] != SER_RECORD_TYPE_TAG) {
                    throw new AlgebricksException("Field accessor is not defined for values of type "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serRecord[0]));
                }
                return 0;
            }

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                try {
                    outInput0.reset();
                    eval0.evaluate(tuple);
                    outInput1.reset();
                    eval1.evaluate(tuple);

                    int subFieldIndex = -1;
                    int subFieldOffset = -1;
                    int subFieldLength = -1;
                    int nullBitmapSize = -1;
                    IAType subType = recordType;
                    ATypeTag subTypeTag = ATypeTag.NULL;
                    byte[] subRecord = outInput0.getByteArray();
                    boolean openField = false;
                    int i = 0;

                    if (checkType(subRecord) == -1) {
                        return;
                    }

                    //Moving through closed fields
                    for (; i < fieldPath.size(); i++) {
                        if (subType.getTypeTag().equals(ATypeTag.UNION)) {
                            //enforced SubType
                            subType = ((AUnionType) subType).getUnionList().get(
                                    AUnionType.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                            if (subType.getTypeTag().serialize() != SER_RECORD_TYPE_TAG) {
                                throw new AlgebricksException("Field accessor is not defined for values of type "
                                        + subTypeTag);
                            }

                        }
                        subFieldIndex = ((ARecordType) subType).findFieldPosition(fieldPath.get(i));
                        if (subFieldIndex == -1) {
                            break;
                        }
                        nullBitmapSize = ARecordType.computeNullBitmapSize((ARecordType) subType);
                        subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetById(subRecord, subFieldIndex,
                                nullBitmapSize, ((ARecordType) subType).isOpen());
                        if (subFieldOffset == 0) {
                            // the field is null, we checked the null bit map
                            out.writeByte(SER_NULL_TYPE_TAG);
                            return;
                        }
                        subType = ((ARecordType) subType).getFieldTypes()[subFieldIndex];
                        if (subType.getTypeTag().equals(ATypeTag.UNION)) {
                            if (NonTaggedFormatUtil.isOptionalField((AUnionType) subType)) {
                                subTypeTag = ((AUnionType) subType).getUnionList()
                                        .get(AUnionType.OPTIONAL_TYPE_INDEX_IN_UNION_LIST).getTypeTag();
                                subFieldLength = NonTaggedFormatUtil.getFieldValueLength(subRecord, subFieldOffset,
                                        subTypeTag, false);
                            } else {
                                // union .. the general case
                                throw new NotImplementedException();
                            }
                        } else {
                            subTypeTag = subType.getTypeTag();
                            subFieldLength = NonTaggedFormatUtil.getFieldValueLength(subRecord, subFieldOffset,
                                    subTypeTag, false);
                        }

                        if (i < fieldPath.size() - 1) {
                            //setup next iteration
                            subRecordTmpStream.reset();
                            subRecordTmpStream.write(subTypeTag.serialize());
                            subRecordTmpStream.write(subRecord, subFieldOffset, subFieldLength);
                            subRecord = subRecordTmpStream.getByteArray();

                            if (checkType(subRecord) == -1) {
                                return;
                            }
                        }
                    }

                    //Moving through open fields
                    for (; i < fieldPath.size(); i++) {
                        openField = true;
                        subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetByName(subRecord,
                                abvs[i].getByteArray());
                        if (subFieldOffset < 0) {
                            out.writeByte(SER_NULL_TYPE_TAG);
                            return;
                        }

                        subTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(subRecord[subFieldOffset]);
                        subFieldLength = NonTaggedFormatUtil.getFieldValueLength(subRecord, subFieldOffset, subTypeTag,
                                true) + 1;

                        if (i < fieldPath.size() - 1) {
                            //setup next iteration
                            subRecord = Arrays.copyOfRange(subRecord, subFieldOffset, subFieldOffset + subFieldLength);

                            if (checkType(subRecord) == -1) {
                                return;
                            }
                        }
                    }
                    if (!openField) {
                        out.writeByte(subTypeTag.serialize());
                    }
                    out.write(subRecord, subFieldOffset, subFieldLength);

                } catch (IOException e) {
                    throw new AlgebricksException(e);
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }
}
