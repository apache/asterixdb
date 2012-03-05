package edu.uci.ics.asterix.om.constants;

import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class AsterixConstantValue implements IAlgebricksConstantValue {

    private IAObject object;

    public AsterixConstantValue(IAObject object) {
        this.setObject(object);
    }

    @Override
    public boolean isFalse() {
        return object == ABoolean.FALSE;
    }

    @Override
    public boolean isNull() {
        return object == ANull.NULL;
    }

    @Override
    public boolean isTrue() {
        return object == ABoolean.TRUE;
    }

    public void setObject(IAObject object) {
        this.object = object;
    }

    public IAObject getObject() {
        return object;
    }

    @Override
    public String toString() {
        return object.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AsterixConstantValue)) {
            return false;
        }
        AsterixConstantValue v2 = (AsterixConstantValue) o;
        return object.deepEqual(v2.getObject());
    }

    @Override
    public int hashCode() {
        return object.hash();
    }
}
