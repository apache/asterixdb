package edu.uci.ics.hyracks.storage.am.common.impls;

import edu.uci.ics.hyracks.storage.am.common.api.IIndexIdProvider;

public class IndexIdProvider implements IIndexIdProvider {
    
    private final byte[] indexId;

    public IndexIdProvider(int indexId) {
        this.indexId = intToByteArray(indexId);
    }
    
    @Override
    public byte[] getIndexId() {
        return indexId;
    }
    
    private byte[] intToByteArray(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((value >>> 24) & 0xFF);
        bytes[1] = (byte) ((value >>> 16) & 0xFF);
        bytes[2] = (byte) ((value >>> 8) & 0xFF);
        bytes[3] = (byte) ((value >>> 0) & 0xFF);
        return bytes;
    }
}
