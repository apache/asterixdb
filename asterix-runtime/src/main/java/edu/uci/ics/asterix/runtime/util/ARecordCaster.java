package edu.uci.ics.asterix.runtime.util;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.dataflow.data.nontagged.AqlNullWriterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;

public class ARecordCaster {

    // describe closed fields in the required type
    private int[] fieldPermutation;

    // describe fields (open or not) in the input records
    private boolean[] openFields;
    private boolean[] optionalFields;

    private List<SimpleValueReference> reqFieldNames = new ArrayList<SimpleValueReference>();
    private List<SimpleValueReference> reqFieldTypeTags = new ArrayList<SimpleValueReference>();
    private ARecordType cachedReqType = null;

    private byte[] buffer = new byte[32768];
    private ResetableByteArrayOutputStream bos = new ResetableByteArrayOutputStream();
    private DataOutputStream dos = new DataOutputStream(bos);

    private RecordBuilder recBuilder = new RecordBuilder();
    private SimpleValueReference nullReference = new SimpleValueReference();
    private SimpleValueReference nullTypeTag = new SimpleValueReference();

    private int numInputFields = 0;

    public ARecordCaster() {
        try {
            bos.setByteArray(buffer, 0);
            int start = bos.size();
            INullWriter nullWriter = AqlNullWriterFactory.INSTANCE.createNullWriter();
            nullWriter.writeNull(dos);
            int end = bos.size();
            nullReference.reset(buffer, start, end - start);
            start = bos.size();
            dos.write(ATypeTag.NULL.serialize());
            end = bos.size();
            nullTypeTag.reset(buffer, start, end);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void castRecord(ARecordAccessor recordAccessor, ARecordType reqType, DataOutput output) throws IOException {
        List<SimpleValueReference> fieldNames = recordAccessor.getFieldNames();
        List<SimpleValueReference> fieldTypeTags = recordAccessor.getFieldTypeTags();
        List<SimpleValueReference> fieldValues = recordAccessor.getFieldValues();
        numInputFields = recordAccessor.getCursor() + 1;

        if (openFields == null || numInputFields > openFields.length) {
            openFields = new boolean[numInputFields];
        }
        if (cachedReqType == null || !reqType.equals(cachedReqType)) {
            loadRequiredType(reqType);
        }

        // clear the previous states
        reset();
        matchClosedPart(fieldNames, fieldTypeTags, fieldValues);
        writeOutput(fieldNames, fieldTypeTags, fieldValues, output);
    }

    private void reset() {
        for (int i = 0; i < numInputFields; i++)
            openFields[i] = true;
        for (int i = 0; i < fieldPermutation.length; i++)
            fieldPermutation[i] = -1;
    }

    private void loadRequiredType(ARecordType reqType) throws IOException {
        reqFieldNames.clear();
        reqFieldTypeTags.clear();

        cachedReqType = reqType;
        int numSchemaFields = reqType.getFieldTypes().length;
        IAType[] fieldTypes = reqType.getFieldTypes();
        String[] fieldNames = reqType.getFieldNames();
        fieldPermutation = new int[numSchemaFields];
        optionalFields = new boolean[numSchemaFields];
        for (int i = 0; i < optionalFields.length; i++)
            optionalFields[i] = false;

        bos.setByteArray(buffer, nullReference.getStartIndex() + nullReference.getLength());
        for (int i = 0; i < numSchemaFields; i++) {
            ATypeTag ftypeTag = fieldTypes[i].getTypeTag();
            String fname = fieldNames[i];

            // add type tag pointable
            if (fieldTypes[i].getTypeTag() == ATypeTag.UNION
                    && NonTaggedFormatUtil.isOptionalField((AUnionType) fieldTypes[i])) {
                // optional field: add the embedded non-null type tag
                ftypeTag = ((AUnionType) fieldTypes[i]).getUnionList()
                        .get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST).getTypeTag();
                optionalFields[i] = true;
            }
            int tagStart = bos.size();
            dos.writeByte(ftypeTag.serialize());
            int tagEnd = bos.size();
            SimpleValueReference typeTagPointable = new SimpleValueReference();
            typeTagPointable.reset(buffer, tagStart, tagEnd - tagStart);
            reqFieldTypeTags.add(typeTagPointable);

            // add type name pointable (including a string type tag)
            int nameStart = bos.size();
            dos.write(ATypeTag.STRING.serialize());
            dos.writeUTF(fname);
            int nameEnd = bos.size();
            SimpleValueReference typeNamePointable = new SimpleValueReference();
            typeNamePointable.reset(buffer, nameStart, nameEnd - nameStart);
            reqFieldNames.add(typeNamePointable);
        }
    }

    private void matchClosedPart(List<SimpleValueReference> fieldNames, List<SimpleValueReference> fieldTypeTags,
            List<SimpleValueReference> fieldValues) {
        // forward match: match from actual to required
        boolean matched = false;
        for (int i = 0; i < numInputFields; i++) {
            SimpleValueReference fieldName = fieldNames.get(i);
            SimpleValueReference fieldTypeTag = fieldTypeTags.get(i);
            matched = false;
            for (int j = 0; j < reqFieldNames.size(); j++) {
                SimpleValueReference reqFieldName = reqFieldNames.get(j);
                SimpleValueReference reqFieldTypeTag = reqFieldTypeTags.get(j);
                if (fieldName.equals(reqFieldName) && fieldTypeTag.equals(reqFieldTypeTag)) {
                    fieldPermutation[j] = i;
                    openFields[i] = false;
                    matched = true;
                    break;
                }
            }
            if (matched)
                continue;
            // the input has extra fields
            if (!cachedReqType.isOpen())
                throw new IllegalStateException("type mismatch: including extra closed fields");
        }

        // backward match: match from required to actual
        for (int i = 0; i < reqFieldNames.size(); i++) {
            SimpleValueReference reqFieldName = reqFieldNames.get(i);
            SimpleValueReference reqFieldTypeTag = reqFieldTypeTags.get(i);
            matched = false;
            for (int j = 0; j < numInputFields; j++) {
                SimpleValueReference fieldName = fieldNames.get(j);
                SimpleValueReference fieldTypeTag = fieldTypeTags.get(j);
                if (fieldName.equals(reqFieldName)) {
                    if (fieldTypeTag.equals(reqFieldTypeTag)) {
                        matched = true;
                        break;
                    }

                    // match the null type of optional field
                    if (optionalFields[i] && fieldTypeTag.equals(nullTypeTag)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (matched)
                continue;

            IAType t = cachedReqType.getFieldTypes()[i];
            if (!(t.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t))) {
                // no matched field in the input for a required closed field
                throw new IllegalStateException("type mismatch: miss a required closed field");
            }
        }
    }

    private void writeOutput(List<SimpleValueReference> fieldNames, List<SimpleValueReference> fieldTypeTags,
            List<SimpleValueReference> fieldValues, DataOutput output) throws IOException {
        // reset the states of the record builder
        recBuilder.reset(cachedReqType);
        recBuilder.init();

        // write the closed part
        for (int i = 0; i < fieldPermutation.length; i++) {
            int pos = fieldPermutation[i];
            SimpleValueReference field;
            if (pos >= 0) {
                field = fieldValues.get(pos);
            } else {
                field = nullReference;
            }
            recBuilder.addField(i, field);
        }

        // write the open part
        for (int i = 0; i < numInputFields; i++) {
            if (openFields[i]) {
                SimpleValueReference name = fieldNames.get(i);
                SimpleValueReference field = fieldValues.get(i);
                recBuilder.addField(name, field);
            }
        }
        recBuilder.write(output, true);
    }
}
