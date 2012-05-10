package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ADateTime implements IAObject {

    protected long chrononTime;

    public ADateTime(long chrononTime) {
        this.chrononTime = chrononTime;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ADATETIME;
    }

    public int compare(Object o) {
        if (!(o instanceof ADateTime)) {
            return -1;
        }

        ADateTime d = (ADateTime) o;
        if (this.chrononTime > d.chrononTime) {
            return 1;
        } else if (this.chrononTime < d.chrononTime) {
            return -1;
        } else {
            return 0;
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof ADateTime)) {
            return false;
        } else {
            ADateTime t = (ADateTime) o;
            return t.chrononTime == this.chrononTime;
        }
    }

    @Override
    public int hashCode() {
        return (int) (chrononTime ^ (chrononTime >>> 32));
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
        StringBuilder sbder = new StringBuilder();
        sbder.append("ADateTime: { ");
        GregorianCalendarSystem.getInstance().getStringRep(chrononTime, sbder);
        sbder.append(" }");
        return sbder.toString();
    }

    public long getChrnonoTime() {
        return chrononTime;
    }

}
