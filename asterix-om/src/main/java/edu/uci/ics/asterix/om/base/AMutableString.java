package edu.uci.ics.asterix.om.base;

public class AMutableString extends AString {

    public AMutableString(String value) {
        super(value);
    }

    public void setValue(String value) {
        this.value = value;
    }
}
