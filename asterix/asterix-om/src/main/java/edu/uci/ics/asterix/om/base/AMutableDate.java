package edu.uci.ics.asterix.om.base;

public class AMutableDate extends ADate {

    public AMutableDate(int year, int month, int day, int timezone) {
        super(year, month, day, timezone);
    }

    public void setValue(int year, int month, int day, int timezone) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.timezone = timezone;
    }

}
