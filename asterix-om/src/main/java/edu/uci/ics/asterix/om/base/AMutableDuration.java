package edu.uci.ics.asterix.om.base;

public class AMutableDuration extends ADuration {

    public AMutableDuration(int months, long milliseconds) {
        super(months, milliseconds);
    }

    public void setValue(int months, long milliseconds) {
        this.chrononInMonth = months;
        this.chrononInMillisecond = milliseconds;
    }

}
