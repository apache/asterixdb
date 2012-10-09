package edu.uci.ics.hyracks.algebricks.examples.piglet.compiler;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.examples.piglet.types.Type;

public final class ConstantValue implements IAlgebricksConstantValue {
    private final Type type;

    private final String image;

    public ConstantValue(Type type, String image) {
        this.type = type;
        this.image = image;
    }

    public Type getType() {
        return type;
    }

    public String getImage() {
        return image;
    }

    @Override
    public boolean isFalse() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isTrue() {
        return false;
    }
}
