package edu.uci.ics.asterix.om.base;

public class AMutableDate extends ADate {

    public AMutableDate(int chrononTimeInDays) {
        super(chrononTimeInDays);
    }

    public void setValue(int chrononTimeInDays) {
        this.chrononTimeInDay = chrononTimeInDays;
    }

}
