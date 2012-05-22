package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

/**
 * ADate type represents dates in a gregorian calendar system.
 */
public class ADate implements IAObject {

    protected int chrononTimeInDay;

    private static long CHRONON_OF_DAY = 24 * 60 * 60 * 1000;

    public ADate(int chrononTimeInDay) {
        this.chrononTimeInDay = chrononTimeInDay;
    }

    public IAType getType() {
        return BuiltinType.ADATE;
    }

    public boolean equals(Object o) {
        if (!(o instanceof ADate)) {
            return false;
        } else {
            return ((ADate) o).chrononTimeInDay == this.chrononTimeInDay;
        }
    }

    @Override
    public int hashCode() {
        return chrononTimeInDay;
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
        StringBuilder sbder = new StringBuilder();
        sbder.append("ADate: { ");
        GregorianCalendarSystem.getInstance().getExtendStringRepWithTimezoneUntilField(
                chrononTimeInDay * CHRONON_OF_DAY, 0, sbder, GregorianCalendarSystem.YEAR, GregorianCalendarSystem.DAY);
        sbder.append(" }");
        return sbder.toString();
    }

    public int getChrononTimeInDays() {
        return chrononTimeInDay;
    }
}
