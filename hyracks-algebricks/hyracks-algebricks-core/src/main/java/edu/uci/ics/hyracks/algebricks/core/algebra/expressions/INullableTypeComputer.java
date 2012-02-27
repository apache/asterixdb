package edu.uci.ics.hyracks.algebricks.core.algebra.expressions;

import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public interface INullableTypeComputer {
    public Object makeNullableType(Object type) throws AlgebricksException;
}
