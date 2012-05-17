package edu.uci.ics.asterix.runtime.accessors;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.runtime.accessors.base.IBinaryAccessor;
import edu.uci.ics.asterix.runtime.accessors.visitor.IBinaryAccessorVisitor;

public class AListAccessor implements IBinaryAccessor {

    @Override
    public byte[] getBytes() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getLength() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getStartIndex() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void reset(byte[] b, int start, int len) {
        // TODO Auto-generated method stub

    }

    @Override
    public <R, T> R accept(IBinaryAccessorVisitor<R, T> vistor, T tag) throws AsterixException {
        return vistor.visit(this, tag);
    }

}
