package edu.uci.ics.asterix.builders;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class RecordBuilder implements IARecordBuilder {
    private int openPartOffset;

    private ARecordType recType;

    private ByteArrayOutputStream closedPartOutputStream;
    private int[] closedPartOffsets;
    private int numberOfClosedFields;
    private byte[] nullBitMap;
    private int nullBitMapSize;

    private ByteArrayOutputStream openPartOutputStream;
    private long[] openPartOffsets;
    private long[] tempOpenPartOffsets;

    private int numberOfOpenFields;

    private int fieldNameHashCode;
    private final IBinaryHashFunction utf8HashFunction;

    // for write()
    private int openPartOffsetArraySize;
    private byte[] openPartOffsetArray;
    private int offsetPosition;
    private int headerSize;
    private boolean isOpen;
    private boolean isNullable;
    private int numberOfSchemaFields;
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

    public RecordBuilder() {

        this.closedPartOutputStream = new ByteArrayOutputStream();
        this.numberOfClosedFields = 0;

        this.openPartOutputStream = new ByteArrayOutputStream();
        this.openPartOffsets = new long[20];
        this.tempOpenPartOffsets = new long[20];

        this.numberOfOpenFields = 0;

        this.fieldNameHashCode = 0;
        this.utf8HashFunction = new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY)
                .createBinaryHashFunction();

        // for write()
        this.openPartOffsetArray = null;
        this.openPartOffsetArraySize = 0;
        this.offsetPosition = 0;
    }

    @Override
    public void init() {
        this.numberOfClosedFields = 0;
        this.closedPartOutputStream.reset();
        this.openPartOutputStream.reset();
        this.numberOfOpenFields = 0;
        this.offsetPosition = 0;
        if (nullBitMap != null) {
            Arrays.fill(nullBitMap, (byte) 0);
        }
    }

    @Override
    public void reset(ARecordType recType) {
        this.recType = recType;
        this.closedPartOutputStream.reset();
        this.numberOfClosedFields = 0;
        if (recType != null) {
            this.isOpen = recType.isOpen();
            this.isNullable = NonTaggedFormatUtil.hasNullableField(recType);
            this.numberOfSchemaFields = recType.getFieldNames().length;
        } else {
            this.isOpen = true;
            this.isNullable = false;
            this.numberOfSchemaFields = 0;
        }
        // the header of the record consists of tag + record length (in bytes) +
        // boolean to indicates whether or not the record is expanded + the
        // offset of the open part of the record + the number of closed fields +
        // null bit map for closed fields.

        headerSize = 5;
        // this is the record tag (1) + record size (4)

        if (isOpen)
            // to tell whether the record has an open part or not.
            headerSize += 1;

        if (numberOfSchemaFields > 0) {
            // there is a schema associated with the record.

            headerSize += 4;
            // 4 = four bytes to store the number of closed fields.
            if (closedPartOffsets == null || closedPartOffsets.length < numberOfSchemaFields)
                closedPartOffsets = new int[numberOfSchemaFields];

            if (isNullable) {
                nullBitMapSize = (int) Math.ceil(numberOfSchemaFields / 8.0);
                if (nullBitMap == null || nullBitMap.length < nullBitMapSize)
                    this.nullBitMap = new byte[nullBitMapSize];
                Arrays.fill(nullBitMap, 0, nullBitMapSize, (byte) 0);
                headerSize += nullBitMap.length;
            }
        }
    }

    @Override
    public void addField(int id, IValueReference value) {
        closedPartOffsets[id] = closedPartOutputStream.size();
        int len = value.getLength() - 1;
        // +1 because we do not store the value tag.
        closedPartOutputStream.write(value.getByteArray(), value.getStartOffset() + 1, len);
        numberOfClosedFields++;
        if (isNullable && value.getByteArray()[0] != SER_NULL_TYPE_TAG) {
            nullBitMap[id / 8] |= (byte) (1 << (7 - (id % 8)));
        }
    }

    @Override
    public void addField(IValueReference name, IValueReference value) {
        if (numberOfOpenFields == openPartOffsets.length) {
            tempOpenPartOffsets = openPartOffsets;
            openPartOffsets = new long[numberOfOpenFields + 20];
            for (int i = 0; i < tempOpenPartOffsets.length; i++)
                openPartOffsets[i] = tempOpenPartOffsets[i];
        }
        fieldNameHashCode = utf8HashFunction.hash(name.getByteArray(), name.getStartOffset() + 1, name.getLength());
        openPartOffsets[this.numberOfOpenFields] = fieldNameHashCode;
        openPartOffsets[this.numberOfOpenFields] = (openPartOffsets[numberOfOpenFields] << 32);
        openPartOffsets[numberOfOpenFields++] += openPartOutputStream.size();
        openPartOutputStream.write(name.getByteArray(), name.getStartOffset() + 1, name.getLength() - 1);
        openPartOutputStream.write(value.getByteArray(), value.getStartOffset(), value.getLength());
    }

    @Override
    public void write(DataOutput out, boolean writeTypeTag) throws IOException {
        int h = headerSize;
        int recordLength;
        // prepare the open part
        if (numberOfOpenFields > 0) {
            h += 4;
            // 4 = four bytes to store the offset to the open part.
            openPartOffsetArraySize = numberOfOpenFields * 8;
            if (openPartOffsetArray == null || openPartOffsetArray.length < openPartOffsetArraySize)
                openPartOffsetArray = new byte[openPartOffsetArraySize];

            Arrays.sort(this.openPartOffsets, 0, numberOfOpenFields);

            openPartOffset = h + numberOfSchemaFields * 4 + closedPartOutputStream.size();
            for (int i = 0; i < numberOfOpenFields; i++) {
                fieldNameHashCode = (int) (openPartOffsets[i] >> 32);
                SerializerDeserializerUtil.writeIntToByteArray(openPartOffsetArray, (int) fieldNameHashCode,
                        offsetPosition);
                int fieldOffset = (int) ((openPartOffsets[i] << 64) >> 64);
                SerializerDeserializerUtil.writeIntToByteArray(openPartOffsetArray, fieldOffset + openPartOffset + 4
                        + openPartOffsetArraySize, offsetPosition + 4);
                offsetPosition += 8;
            }
            recordLength = openPartOffset + 4 + openPartOffsetArraySize + openPartOutputStream.size();
        } else
            recordLength = h + numberOfSchemaFields * 4 + closedPartOutputStream.size();

        // write the record header
        if (writeTypeTag) {
            out.writeByte(RECORD_TYPE_TAG);
        }
        out.writeInt(recordLength);
        if (isOpen) {
            if (this.numberOfOpenFields > 0) {
                out.writeBoolean(true);
                out.writeInt(openPartOffset);
            } else
                out.writeBoolean(false);
        }

        // write the closed part
        if (numberOfSchemaFields > 0) {
            out.writeInt(numberOfClosedFields);
            if (isNullable)
                out.write(nullBitMap, 0, nullBitMapSize);
            for (int i = 0; i < numberOfSchemaFields; i++)
                out.writeInt(closedPartOffsets[i] + h + (numberOfSchemaFields * 4));
            out.write(closedPartOutputStream.toByteArray());
        }

        // write the open part
        if (numberOfOpenFields > 0) {
            out.writeInt(numberOfOpenFields);
            out.write(openPartOffsetArray, 0, openPartOffsetArraySize);
            out.write(openPartOutputStream.toByteArray());
        }
    }

    @Override
    public int getFieldId(String fieldName) {
        for (int i = 0; i < recType.getFieldNames().length; i++) {
            if (recType.getFieldNames()[i].equals(fieldName))
                return i;
        }
        return -1;
    }

}