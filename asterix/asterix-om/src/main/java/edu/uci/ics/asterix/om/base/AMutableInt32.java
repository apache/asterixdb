package edu.uci.ics.asterix.om.base;

public class AMutableInt32 extends AInt32 {

    public AMutableInt32(int value) {
        super(value);
    }

    public void setValue(int value) {
        this.value = value;
    }

}
