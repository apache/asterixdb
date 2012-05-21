package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ATime implements IAObject {

    protected int ora;
    
    public ATime(int ora) {
        this.ora = ora;
    }
    
    @Override
    public IAType getType() {
        return BuiltinType.ATIME;
    }
      
    public int compare(Object o) {
        if (!(o instanceof ATime)) {
            return -1;
        }

        ATime d = (ATime) o;
        if (this.ora > d.ora) {
            return 1;
        } else if (this.ora < d.ora) {
            return -1;
        } else {
            return 0;
        }
    }
      
    @Override
    public boolean equals(Object o) {
    	
        if (!(o instanceof ATime)) {
            return false;
        } else {
            ATime t = (ATime) o;
            return t.ora == this.ora;
            
        }
    }

    @Override
    public int hashCode() {
    	return ora;
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
    	StringBuilder sbder = new StringBuilder();
        sbder.append("ATime: { ");
        GregorianCalendarSystem.getInstance().getStringRepTime(ora, sbder);
        sbder.append(" }");
        return sbder.toString();

    }
    
    public int getOra() {
    	return ora;
    }
    
}
