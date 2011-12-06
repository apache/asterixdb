package edu.uci.ics.hyracks.algebricks.core.algebra.expressions;

public interface IAlgebricksConstantValue {
    public boolean isNull();

    public boolean isTrue();

    public boolean isFalse();
}
