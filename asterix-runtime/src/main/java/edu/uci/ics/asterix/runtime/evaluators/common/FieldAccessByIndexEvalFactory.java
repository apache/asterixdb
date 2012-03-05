package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class FieldAccessByIndexEvalFactory implements IEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IEvaluatorFactory recordEvalFactory;
    private IEvaluatorFactory fieldIndexEvalFactory;
    private int nullBitmapSize;
    private ARecordType recordType;
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

    public FieldAccessByIndexEvalFactory(IEvaluatorFactory recordEvalFactory, IEvaluatorFactory fieldIndexEvalFactory,
            ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.fieldIndexEvalFactory = fieldIndexEvalFactory;
        this.recordType = recordType;
        if (NonTaggedFormatUtil.hasNullableField(recordType))
            this.nullBitmapSize = (int) Math.ceil(recordType.getFieldNames().length / 8.0);
        else
            this.nullBitmapSize = 0;

    }

    @Override
    public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new IEvaluator() {

            private DataOutput out = output.getDataOutput();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private IEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private IEvaluator eval1 = fieldIndexEvalFactory.createEvaluator(outInput1);
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
                    byte[] serRecord = outInput0.getBytes();

                    if (serRecord[0] == SER_NULL_TYPE_TAG) {
                        nullSerde.serialize(ANull.NULL, out);
                        return;
                    }

                    if (serRecord[0] != SER_RECORD_TYPE_TAG) {
                        throw new AlgebricksException("Field accessor is not defined for values of type "
                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serRecord[0]));
                    }

                    fieldIndex = IntegerSerializerDeserializer.getInt(outInput1.getBytes(), 1);
                    fieldValueOffset = ARecordSerializerDeserializer.getFieldOffsetById(serRecord, fieldIndex,
                            nullBitmapSize, recordType.isOpen());

                    if (fieldValueOffset == 0) {
                        // the field is null, we checked the null bit map
                        out.writeByte(SER_NULL_TYPE_TAG);
                        return;
                    }

                    fieldValueType = recordType.getFieldTypes()[fieldIndex];
                    if (fieldValueType.getTypeTag().equals(ATypeTag.UNION)) {
                        if (NonTaggedFormatUtil.isOptionalField((AUnionType) fieldValueType)) {
                            fieldValueTypeTag = ((AUnionType) fieldValueType).getUnionList()
                                    .get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST).getTypeTag();
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
