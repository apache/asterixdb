package edu.uci.ics.hivesterix.logical.expression;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class HivesterixConstantValue implements IAlgebricksConstantValue {

    private Object object;

    public HivesterixConstantValue(Object object) {
        this.setObject(object);
    }

    @Override
    public boolean isFalse() {
        return object == Boolean.FALSE;
    }

    @Override
    public boolean isNull() {
        return object == null;
    }

    @Override
    public boolean isTrue() {
        return object == Boolean.TRUE;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public Object getObject() {
        return object;
    }

    @Override
    public String toString() {
        return object.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HivesterixConstantValue)) {
            return false;
        }
        HivesterixConstantValue v2 = (HivesterixConstantValue) o;
        return object.equals(v2.getObject());
    }

    @Override
    public int hashCode() {
        return object.hashCode();
    }

}
