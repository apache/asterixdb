package edu.uci.ics.asterix.om.base;

public class AMutableDateTime extends ADateTime {

    public AMutableDateTime(int year, int month, int day, int hour, int minutes, int seconds, int milliseconds,
            int microseconds, int timezone) {
        super(year, month, day, hour, minutes, seconds, milliseconds, microseconds, timezone);
    }

    public void setValue(int year, int month, int day, int hour, int minutes, int seconds, int milliseconds,
            int microseconds, int timezone) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minutes = minutes;
        this.seconds = seconds;
        this.milliseconds = milliseconds;
        this.microseconds = microseconds;
        this.timezone = timezone;
    }

}
