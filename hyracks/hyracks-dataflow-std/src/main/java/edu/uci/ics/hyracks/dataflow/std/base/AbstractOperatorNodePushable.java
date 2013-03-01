package edu.uci.ics.hyracks.dataflow.std.base;

import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;

public abstract class AbstractOperatorNodePushable implements IOperatorNodePushable {
    @Override
    public String getDisplayName() {
        return toString();
    }
}