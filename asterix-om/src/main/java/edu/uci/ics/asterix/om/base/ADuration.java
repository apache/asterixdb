package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ADuration implements IAObject {

    protected int chrononInMonth;
    protected long chrononInMillisecond;

    public ADuration(int months, long seconds) {
        this.chrononInMonth = months;
        this.chrononInMillisecond = seconds;
    }

    public int getMonths() {
        return chrononInMonth;
    }

    public long getMilliseconds() {
        return chrononInMillisecond;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ADURATION;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADuration)) {
            return false;
        } else {
            ADuration d = (ADuration) o;
            return d.getMonths() == chrononInMonth && d.getMilliseconds() == chrononInMillisecond;
        }
    }

    @Override
    public int hashCode() {
        return (int) (chrononInMonth ^ (chrononInMillisecond) ^ (chrononInMillisecond >>> 32));
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitADuration(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

}
