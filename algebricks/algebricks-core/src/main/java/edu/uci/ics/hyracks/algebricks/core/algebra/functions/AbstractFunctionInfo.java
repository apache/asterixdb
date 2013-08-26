package edu.uci.ics.hyracks.algebricks.core.algebra.functions;

import java.io.Serializable;

public abstract class AbstractFunctionInfo implements IFunctionInfo, Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean isFunctional;

    protected AbstractFunctionInfo(boolean isFunctional) {
        this.isFunctional = isFunctional;
    }

    @Override
    public boolean isFunctional() {
        return isFunctional;
    }

}
