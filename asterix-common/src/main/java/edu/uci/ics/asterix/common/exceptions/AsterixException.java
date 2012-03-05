package edu.uci.ics.asterix.common.exceptions;

public class AsterixException extends Exception {
    private static final long serialVersionUID = 1L;

    public AsterixException() {
    }

    public AsterixException(String message) {
        super(message);
    }

    public AsterixException(Throwable cause) {
        super(cause);
    }

    public AsterixException(String message, Throwable cause) {
        super(message, cause);
    }
}