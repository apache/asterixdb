package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ADateTime implements IAObject {

    private static final int[] NUM_DAYS_IN_MONTH = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    protected int day;
    protected int hour;
    protected int minutes;
    protected int month;
    protected int seconds;
    protected int timezone;
    protected int year;
    protected int milliseconds;
    protected int microseconds;

    public ADateTime(int year, int month, int day, int hour, int minutes, int seconds, int timezone) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minutes = minutes;
        this.seconds = seconds;
        this.timezone = timezone;
    }

    public ADateTime(int year, int month, int day, int hour, int minutes, int seconds, int milliseconds,
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

    public int getDay() {
        return day;
    }

    public int getHours() {
        return hour;
    }

    public int getMinutes() {
        return minutes;
    }

    public int getMonth() {
        return month;
    }

    public int getSeconds() {
        return seconds;
    }

    public int getTimeZone() {
        return timezone;
    }

    public int getYear() {
        return year;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ADATETIME;
    }

    public int getMicroseconds() {
        return microseconds;
    }

    public int getMilliseconds() {
        return milliseconds;
    }

    public int compare(Object o) {
        if (!(o instanceof ADateTime)) {
            return -1;
        }

        ADateTime d = (ADateTime) o;
        if (timezone != d.getTimeZone()) {
            AMutableDateTime dt1 = new AMutableDateTime(year, month, day, hour, minutes, seconds, milliseconds,
                    microseconds, timezone);
            AMutableDateTime dt2 = new AMutableDateTime(d.getYear(), d.getMonth(), d.getDay(), d.getHours(),
                    d.getMinutes(), d.getSeconds(), d.getMilliseconds(), d.getMicroseconds(), d.getTimeZone());
            dt1.convertToUTC();
            dt2.convertToUTC();

            return dt1.tzEqualCompare(dt2);

        }

        return tzEqualCompare(d);
    }

    protected void convertToUTC() {
        short tzMin = (short) ((timezone % 4) * 15);
        short tzHr = (short) (timezone / 4);
        minutes -= tzMin;
        if (minutes >= 60) {
            minutes %= 60;
            hour += 1;
        }
        hour -= tzHr;
        if (hour >= 24) {
            hour %= 24;
            day += 1;
        }
        if (isLeapYear(year) && month == 2) {
            if (day >= 29) {
                day %= 29;
                month += 1;
            }
        } else {
            if (day >= NUM_DAYS_IN_MONTH[month - 1]) {
                day %= NUM_DAYS_IN_MONTH[month - 1];
                month += 1;
            }
        }

        if (month >= 12) {
            month %= 12;
            year += 1;
        }
        timezone = 0;

    }

    protected static boolean isLeapYear(int year) {
        if (year % 400 == 0) {
            return true;
        } else if (year % 100 == 0) {
            return false;
        } else if (year % 4 == 0) {
            return true;
        } else {
            return false;
        }
    }

    // comparison that assumes the timezones are the same!
    protected int tzEqualCompare(ADateTime d) {
        if (year != d.getYear()) {
            return year - d.getYear();
        }
        if (month != d.getMonth()) {
            return month - d.getMonth();
        }
        if (day != d.getDay()) {
            return day - d.getDay();
        }
        if (hour != d.getHours()) {
            return hour - d.getHours();
        }
        if (minutes != d.getMinutes()) {
            return minutes - d.getMinutes();
        }
        if (seconds != d.getSeconds()) {
            return seconds - d.getSeconds();
        }
        if (milliseconds != d.getMilliseconds()) {
            return milliseconds - d.getMilliseconds();
        }
        if (microseconds != d.getMicroseconds()) {
            return microseconds - d.getMicroseconds();
        }

        return 0;
    }

    public boolean equals(Object o) {
        if (!(o instanceof ADateTime)) {
            return false;
        } else {
            ADateTime d = (ADateTime) o;
            return d.getMicroseconds() == microseconds && d.getMilliseconds() == milliseconds
                    && d.getSeconds() == seconds && d.getMinutes() == minutes && d.getHours() == hour
                    && d.getDay() == day && d.getMonth() == month && d.getYear() == year && d.getTimeZone() == timezone;
        }
    }

    @Override
    public int hashCode() {
        return (((((((year * 31 + month) * 31 + day) * 31 + timezone) * 31 + hour) * 31 + minutes) * 31 + seconds) * 31 + milliseconds)
                * 31 + microseconds;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitADateTime(this);
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
        return "ADateTime: { " + year + "-" + month + "-" + day + ":" + hour + ":" + minutes + ":" + seconds + ":"
                + milliseconds + ":" + microseconds + ":" + timezone + " }";
    }

}
