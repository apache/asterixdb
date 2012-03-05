package edu.uci.ics.asterix.translator;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class TranslationException extends AsterixException {
    /**
     * 
     */
    private static final long serialVersionUID = 685960054131778068L;

    public TranslationException() {
        super();
    }

    public TranslationException(String msg) {
        super(msg);
    }
}
