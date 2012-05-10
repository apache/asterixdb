package edu.uci.ics.asterix.om.base;

public class AMutableDateTime extends ADateTime {

    public AMutableDateTime(long chrononTime) {
        super(chrononTime);
    }

    public void setValue(long chrononTime) {
        this.chrononTime = chrononTime;
    }

}
