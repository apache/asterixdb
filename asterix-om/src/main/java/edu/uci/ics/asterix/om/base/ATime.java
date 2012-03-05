package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ATime implements IAObject {

    protected int hour;
    protected int minutes;
    protected int seconds;
    protected int timezone;
    protected int milliseconds;
    protected int microseconds;

    public ATime(int hour, int minutes, int seconds, int timezone) {
        this.hour = hour;
        this.minutes = minutes;
        this.seconds = seconds;
        this.timezone = timezone;
    }

    public ATime(int hour, int minutes, int seconds, int milliseconds, int microseconds, int timezone) {
        this.hour = hour;
        this.minutes = minutes;
        this.seconds = seconds;
        this.milliseconds = milliseconds;
        this.microseconds = microseconds;
        this.timezone = timezone;
    }

    public int getHours() {
        return hour;
    }

    public int getMinutes() {
        return minutes;
    }

    public int getSeconds() {
        return seconds;
    }

    public int getTimeZone() {
        return timezone;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ATIME;
    }

    public int getMicroseconds() {
        return microseconds;
    }

    public int getMilliseconds() {
        return milliseconds;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ATime)) {
            return false;
        } else {
            ATime t = (ATime) o;
            return t.getMicroseconds() == microseconds && t.getMilliseconds() == milliseconds
                    && t.getSeconds() == seconds && t.getMinutes() == minutes && t.getHours() == hour
                    && t.getTimeZone() == timezone;
        }
    }

    @Override
    public int hashCode() {
        return ((((timezone * 31 + hour) * 31 + minutes) * 31 + seconds) * 31 + milliseconds) * 31 + microseconds;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitATime(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public String toString() {
        return "ATime: { " + hour + ":" + minutes + ":" + seconds + ":" + milliseconds + ":" + microseconds + ":"
                + timezone + " }";
    }
}
