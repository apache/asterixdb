package edu.uci.ics.asterix.om.base;

public class AMutableTime extends ATime {

    public AMutableTime(int timeInMillisec) {
        super(timeInMillisec);
    }

    public void setValue(int timeInMillisec) {
        this.chrononTime = timeInMillisec;
    }

}
