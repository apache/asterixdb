package edu.uci.ics.hyracks.storage.am.btree.exceptions;

public class BTreeNotUpdateableException extends BTreeException {
    private static final long serialVersionUID = 1L;
    
    public BTreeNotUpdateableException(Exception e) {
        super(e);
    }
    
    public BTreeNotUpdateableException(String message) {
        super(message);
    }
}
