package edu.uci.ics.asterix.om.base;

public class AMutableTime extends ATime {

    public AMutableTime(int ora) {
        super(ora);
    }

    public void setValue(int ora) {
        this.chrononTime = ora;
    }

}
