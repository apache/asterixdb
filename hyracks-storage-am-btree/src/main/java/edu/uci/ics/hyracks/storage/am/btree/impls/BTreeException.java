package edu.uci.ics.hyracks.storage.am.btree.impls;

public class BTreeException extends Exception {
	
	private static final long serialVersionUID = 1L;
	private boolean handled = false;
		
	public BTreeException(String message) {
        super(message);
    }
	
	public void setHandled(boolean handled) {
		this.handled = handled;
	}
	
	public boolean getHandled() {
		return handled;
	}	
}
