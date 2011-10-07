package edu.uci.ics.hyracks.storage.am.btree.exceptions;

public class BTreeNonExistentKeyException extends BTreeException {
    
    private static final long serialVersionUID = 1L;
    
    public BTreeNonExistentKeyException(Exception e) {
        super(e);
    }
    
    public BTreeNonExistentKeyException(String message) {
        super(message);
    }
}
