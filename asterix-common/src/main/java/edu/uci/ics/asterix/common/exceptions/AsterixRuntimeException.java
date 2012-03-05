package edu.uci.ics.asterix.common.exceptions;

public class AsterixRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public AsterixRuntimeException() {
        super();
    }

    public AsterixRuntimeException(String msg) {
        super(msg);
    }

    public AsterixRuntimeException(Throwable cause) {
        super(cause);
    }

}
