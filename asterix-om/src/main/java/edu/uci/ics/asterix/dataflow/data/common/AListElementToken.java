package edu.uci.ics.asterix.dataflow.data.common;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;

public class AListElementToken implements IToken {

    protected byte[] data;
    protected int start;
    protected int length;
    protected int tokenLength;
    protected int typeTag;
    
    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int getStart() {
        return start;
    }

    @Override
    public int getTokenLength() {
        return tokenLength;
    }

    @Override
    public void reset(byte[] data, int start, int length, int tokenLength, int tokenCount) {
        this.data = data;
        this.start = start;
        this.length = length;
        this.tokenLength = tokenLength;
        // We abuse the last param, tokenCount, to pass the type tag.
        typeTag = tokenCount;
    }

    @Override
    public void serializeToken(DataOutput dos) throws IOException {
        dos.writeByte(typeTag);
        dos.write(data, start, length);
    }

    @Override
    public void serializeTokenCount(DataOutput dos) throws IOException {
        throw new UnsupportedOperationException("Token count not implemented.");
    }

}
