package edu.uci.ics.hyracks.storage.am.btree.impls;

public class BTreeDuplicateKeyException extends BTreeException {
    private static final long serialVersionUID = 1L;
    
    public BTreeDuplicateKeyException(Exception e) {
        super(e);
    }
    
    public BTreeDuplicateKeyException(String message) {
        super(message);
    }
}
