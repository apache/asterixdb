package edu.uci.ics.asterix.om.base;

public class AMutableInt64 extends AInt64 {

    public AMutableInt64(long value) {
        super(value);
    }

    public void setValue(long value) {
        this.value = value;
    }

}
