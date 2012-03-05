package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlPrinterFactoryProvider;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class ARecordPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    private final ARecordType recType;

    public ARecordPrinterFactory(ARecordType recType) {
        this.recType = recType;
    }

    @Override
    public IPrinter createPrinter() {

        return new IPrinter() {

            private IPrinter[] fieldPrinters;
            private IAType[] fieldTypes;
            private int[] fieldOffsets;
            private int numberOfSchemaFields, numberOfOpenFields, openPartOffset, fieldOffset, offsetArrayOffset,
                    fieldValueLength, nullBitMapOffset, recordOffset;
            private boolean isExpanded, hasNullableFields;
            private ATypeTag tag;

            @Override
            public void init() throws AlgebricksException {

                numberOfSchemaFields = 0;
                if (recType != null) {
                    numberOfSchemaFields = recType.getFieldNames().length;
                    fieldPrinters = new IPrinter[numberOfSchemaFields];
                    fieldTypes = new IAType[numberOfSchemaFields];
                    fieldOffsets = new int[numberOfSchemaFields];
                    for (int i = 0; i < numberOfSchemaFields; i++) {
                        fieldTypes[i] = recType.getFieldTypes()[i];
                        if (fieldTypes[i].getTypeTag() == ATypeTag.UNION
                                && NonTaggedFormatUtil.isOptionalField((AUnionType) fieldTypes[i]))
                            fieldPrinters[i] = (AqlPrinterFactoryProvider.INSTANCE
                                    .getPrinterFactory(((AUnionType) fieldTypes[i]).getUnionList().get(
                                            NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST))).createPrinter();
                        else
                            fieldPrinters[i] = (AqlPrinterFactoryProvider.INSTANCE.getPrinterFactory(fieldTypes[i]))
                                    .createPrinter();
                        fieldPrinters[i].init();
                    }
                }
            }

            @Override
            public void print(byte[] b, int start, int l, PrintStream ps) throws AlgebricksException {
                ps.print("{ ");
                isExpanded = false;
                openPartOffset = 0;
                int s = start;
                recordOffset = s;
                if (recType == null) {
                    openPartOffset = s + AInt32SerializerDeserializer.getInt(b, s + 6);
                    s += 8;
                    isExpanded = true;
                } else {
                    if (recType.isOpen()) {
                        isExpanded = b[s + 5] == 1 ? true : false;
                        if (isExpanded) {
                            openPartOffset = s + AInt32SerializerDeserializer.getInt(b, s + 6);
                            s += 10;
                        } else
                            s += 6;
                    } else
                        s += 5;
                }
                try {
                    if (numberOfSchemaFields > 0) {
                        s += 4;
                        nullBitMapOffset = 0;
                        hasNullableFields = NonTaggedFormatUtil.hasNullableField(recType);
                        if (hasNullableFields) {
                            nullBitMapOffset = s;
                            offsetArrayOffset = s
                                    + (this.numberOfSchemaFields % 8 == 0 ? numberOfSchemaFields / 8
                                            : numberOfSchemaFields / 8 + 1);
                        } else {
                            offsetArrayOffset = s;
                        }
                        for (int i = 0; i < numberOfSchemaFields; i++) {
                            fieldOffsets[i] = AInt32SerializerDeserializer.getInt(b, offsetArrayOffset) + recordOffset;
                            offsetArrayOffset += 4;
                        }
                        for (int fieldNumber = 0; fieldNumber < numberOfSchemaFields; fieldNumber++) {
                            if (fieldNumber != 0) {
                                ps.print(", ");
                            }
                            ps.print("\"");
                            ps.print(recType.getFieldNames()[fieldNumber]);
                            ps.print("\": ");
                            if (hasNullableFields) {
                                byte b1 = b[nullBitMapOffset + fieldNumber / 8];
                                int p = 1 << (7 - (fieldNumber % 8));
                                if ((b1 & p) == 0) {
                                    ps.print("null");
                                    continue;
                                }
                            }
                            if (fieldTypes[fieldNumber].getTypeTag() == ATypeTag.UNION) {
                                if (NonTaggedFormatUtil.isOptionalField((AUnionType) fieldTypes[fieldNumber])) {
                                    tag = ((AUnionType) fieldTypes[fieldNumber]).getUnionList()
                                            .get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST).getTypeTag();
                                    fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(b,
                                            fieldOffsets[fieldNumber], tag, false);
                                    fieldPrinters[fieldNumber].print(b, fieldOffsets[fieldNumber] - 1,
                                            fieldOffsets[fieldNumber], ps);
                                }
                            } else {
                                tag = fieldTypes[fieldNumber].getTypeTag();
                                fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(b,
                                        fieldOffsets[fieldNumber], tag, false);
                                fieldPrinters[fieldNumber]
                                        .print(b, fieldOffsets[fieldNumber] - 1, fieldValueLength, ps);
                            }
                        }
                        if (isExpanded)
                            ps.print(", ");
                    }
                    if (isExpanded) {
                        numberOfOpenFields = AInt32SerializerDeserializer.getInt(b, openPartOffset);
                        fieldOffset = openPartOffset + 4 + (8 * numberOfOpenFields);
                        for (int i = 0; i < numberOfOpenFields; i++) {
                            // we print the field name
                            fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(b, fieldOffset, ATypeTag.STRING,
                                    false);
                            AStringPrinter.INSTANCE.print(b, fieldOffset - 1, fieldValueLength, ps);
                            fieldOffset += fieldValueLength;
                            ps.print(": ");
                            // now we print the value
                            tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[fieldOffset]);
                            fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(b, fieldOffset, tag, true) + 1;
                            AObjectPrinter.INSTANCE.print(b, fieldOffset, fieldValueLength, ps);
                            fieldOffset += fieldValueLength;
                            if (i + 1 < numberOfOpenFields)
                                ps.print(", ");
                        }
                    }
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
                ps.print(" }");
            }
        };
    }
}
