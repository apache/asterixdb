package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

// We should avoid representing nulls explicitly. This class is for cases when it is
// harder not to.
public class ANull implements IAObject {

    private ANull() {
    }

    public final static ANull NULL = new ANull();

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitANull(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ANULL;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return obj == this;
    }

    @Override
    public int hash() {
        return 0;
    }

    @Override
    public String toString() {
        return "null";
    }
}
