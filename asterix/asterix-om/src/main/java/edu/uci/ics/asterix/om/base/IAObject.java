package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

/**
 * Represents an object in Asterix.
 */
public interface IAObject {
    public IAType getType();

    public void accept(IOMVisitor visitor) throws AsterixException;

    public boolean deepEqual(IAObject obj);

    public int hash();
}