package edu.uci.ics.asterix.om.base;

public class AMutableFloat extends AFloat {

    public AMutableFloat(float value) {
        super(value);
    }

    public void setValue(float value) {
        this.value = value;
    }

}
