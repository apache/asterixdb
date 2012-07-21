package edu.uci.ics.asterix.om.base.temporal;

public class StringCharSequenceAccessor implements ICharSequenceAccessor<String> {

    private String string;
    private int offset;

    @Override
    public char getCharAt(int index) {
        return string.charAt(index + offset);
    }

    public void reset(String obj, int offset) {
        string = obj;
        this.offset = offset;
    }

    @Override
    public int getLength() {
        return string.length() - offset;
    }

}
