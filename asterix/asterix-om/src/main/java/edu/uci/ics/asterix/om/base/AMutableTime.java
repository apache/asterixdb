package edu.uci.ics.asterix.om.base;

public class AMutableTime extends ATime {

    public AMutableTime(int hour, int minutes, int seconds, int milliseconds, int microseconds, int timezone) {
        super(hour, minutes, seconds, milliseconds, microseconds, timezone);
    }

    public void setValue(int hour, int minutes, int seconds, int milliseconds, int microseconds, int timezone) {
        this.hour = hour;
        this.minutes = minutes;
        this.seconds = seconds;
        this.milliseconds = milliseconds;
        this.timezone = timezone;
    }

}
