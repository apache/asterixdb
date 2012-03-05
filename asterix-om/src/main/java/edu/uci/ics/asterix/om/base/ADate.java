package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ADate implements IAObject {

    protected int day;
    protected int month;
    protected int year;
    protected int timezone;

    public ADate(int year, int month, int day, int timezone) {
        this.day = day;
        this.month = month;
        this.year = year;
        this.timezone = timezone;
    }

    public int getDay() {
        return day;
    }

    public int getMonth() {
        return month;
    }

    public int getTimeZone() {
        return timezone;
    }

    public int getYear() {
        return year;
    }

    public IAType getType() {
        return BuiltinType.ADATE;
    }

    public boolean equals(Object o) {
        if (!(o instanceof ADate)) {
            return false;
        } else {
            ADate d = (ADate) o;
            return d.getDay() == day && d.getMonth() == month && d.getYear() == year && d.getTimeZone() == timezone;
        }
    }

    @Override
    public int hashCode() {
        return ((year * 31 + month) * 31 + day) * 31 + timezone;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitADate(this);
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
        return "ADate: { " + year + "-" + month + "-" + day + ":" + timezone + " }";
    }
}
