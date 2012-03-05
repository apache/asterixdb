package edu.uci.ics.asterix.om.base;

public class AMutableDuration extends ADuration {

    public AMutableDuration(int months, int seconds) {
        super(months, seconds);
    }

    public void setValue(int months, int seconds) {
        this.months = months;
        this.seconds = seconds;
    }

}
