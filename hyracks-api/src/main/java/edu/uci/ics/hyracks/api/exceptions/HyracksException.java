package edu.uci.ics.hyracks.api.exceptions;

public class HyracksException extends Exception {
    private static final long serialVersionUID = 1L;

    public HyracksException() {
    }

    public HyracksException(String message) {
        super(message);
    }

    public HyracksException(Throwable cause) {
        super(cause);
    }

    public HyracksException(String message, Throwable cause) {
        super(message, cause);
    }
}