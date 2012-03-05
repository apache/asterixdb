package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ADuration implements IAObject {

    protected int months;
    protected int seconds;

    public ADuration(int months, int seconds) {
        this.months = months;
        this.seconds = seconds;
    }

    public int getMonths() {
        return months;
    }

    public int getSeconds() {
        return seconds;
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
            return d.getMonths() == months && d.getSeconds() == seconds;
        }
    }

    @Override
    public int hashCode() {
        return months * 31 + seconds;
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
