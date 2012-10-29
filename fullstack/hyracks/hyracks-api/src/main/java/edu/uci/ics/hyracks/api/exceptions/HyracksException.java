package edu.uci.ics.hyracks.api.exceptions;

import java.io.IOException;

public class HyracksException extends IOException {
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