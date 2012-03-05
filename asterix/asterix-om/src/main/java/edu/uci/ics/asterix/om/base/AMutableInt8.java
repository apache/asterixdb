package edu.uci.ics.asterix.om.base;

public class AMutableInt8 extends AInt8 {

    public AMutableInt8(byte value) {
        super(value);
    }

    public void setValue(byte value) {
        this.value = value;
    }

}
