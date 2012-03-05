package edu.uci.ics.asterix.om.base;

public class AMutableDouble extends ADouble {

    public AMutableDouble(double value) {
        super(value);
    }

    public void setValue(double value) {
        this.value = value;
    }

}
