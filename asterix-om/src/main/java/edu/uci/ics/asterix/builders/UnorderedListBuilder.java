package edu.uci.ics.asterix.builders;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public class UnorderedListBuilder implements IAUnorderedListBuilder {

    /**
     * 
     */
    private ByteArrayOutputStream outputStream;
    private ArrayList<Short> offsets;
    private int metadataInfoSize;
    private byte[] offsetArray;
    private int offsetPosition;
    private int headerSize;
    private ATypeTag itemTypeTag;
    private byte serNullTypeTag = ATypeTag.NULL.serialize();
    private final static byte UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();

    private boolean fixedSize = false;
    private int numberOfItems;

    public UnorderedListBuilder() {
        this.outputStream = new ByteArrayOutputStream();
        this.offsets = new ArrayList<Short>();
        this.metadataInfoSize = 0;
        this.offsetArray = null;
        this.offsetPosition = 0;
    }

    @Override
    public void reset(AUnorderedListType unorderedlistType) throws HyracksDataException {
        this.outputStream.reset();
        this.offsetArray = null;
        this.offsets.clear();
        this.offsetPosition = 0;
        this.numberOfItems = 0;
        if (unorderedlistType == null || unorderedlistType.getItemType() == null) {
            this.itemTypeTag = ATypeTag.ANY;
            fixedSize = false;
        } else {
            this.itemTypeTag = unorderedlistType.getItemType().getTypeTag();
            fixedSize = NonTaggedFormatUtil.isFixedSizedCollection(unorderedlistType.getItemType());
        }
        headerSize = 2;
        metadataInfoSize = 8;
        // 8 = 4 (# of items) + 4 (the size of the list)
    }

    @Override
    public void addItem(IValueReference item) throws HyracksDataException {
        if (!fixedSize)
            this.offsets.add((short) outputStream.size());
        if (itemTypeTag == ATypeTag.ANY || (itemTypeTag == ATypeTag.NULL && item.getByteArray()[0] == serNullTypeTag)) {
            this.numberOfItems++;
            this.outputStream.write(item.getByteArray(), item.getStartOffset(), item.getLength());
        } else if (item.getByteArray()[0] != serNullTypeTag) {
            this.numberOfItems++;
            this.outputStream.write(item.getByteArray(), item.getStartOffset() + 1, item.getLength() - 1);
        }
    }

    @Override
    public void write(DataOutput out, boolean writeTypeTag) throws HyracksDataException {
        try {
            if (!fixedSize)
                metadataInfoSize += offsets.size() * 4;
            if (offsetArray == null || offsetArray.length < metadataInfoSize)
                offsetArray = new byte[metadataInfoSize];

            SerializerDeserializerUtil.writeIntToByteArray(offsetArray,
                    headerSize + metadataInfoSize + outputStream.size(), offsetPosition);
            SerializerDeserializerUtil.writeIntToByteArray(offsetArray, this.numberOfItems, offsetPosition + 4);

            if (!fixedSize) {
                offsetPosition += 8;
                for (int i = 0; i < offsets.size(); i++) {
                    SerializerDeserializerUtil.writeIntToByteArray(offsetArray, offsets.get(i) + metadataInfoSize
                            + headerSize, offsetPosition);
                    offsetPosition += 4;
                }
            }
            if (writeTypeTag) {
                out.writeByte(UNORDEREDLIST_TYPE_TAG);
            }
            out.writeByte(itemTypeTag.serialize());
            out.write(offsetArray, 0, metadataInfoSize);
            out.write(outputStream.toByteArray(), 0, outputStream.size());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
