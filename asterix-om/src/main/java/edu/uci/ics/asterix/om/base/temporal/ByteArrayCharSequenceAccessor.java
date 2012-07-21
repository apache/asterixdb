package edu.uci.ics.asterix.om.base.temporal;

public class ByteArrayCharSequenceAccessor implements ICharSequenceAccessor<Byte[]> {

    private byte[] string;
    private int offset;
    private int beginOffset;

    @Override
    public char getCharAt(int index) {
        return (char) (string[index + offset + beginOffset]);
    }

    /* The offset is the position of the first letter in the byte array */
    public void reset(byte[] obj, int beginOffset, int offset) {
        string = obj;
        this.offset = offset;
        this.beginOffset = beginOffset;
    }

    @Override
    public int getLength() {
        return ((string[beginOffset - 2] & 0xff) << 8) + ((string[beginOffset - 1] & 0xff) << 0) - offset;
    }

}
