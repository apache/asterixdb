package edu.uci.ics.asterix.om.base;

public class AMutableInt16 extends AInt16 {

    public AMutableInt16(short value) {
        super(value);
    }

    public void setValue(short value) {
        this.value = value;
    }

}
