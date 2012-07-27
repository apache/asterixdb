package edu.uci.ics.asterix.runtime.aggregates.base;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public interface IAccumulator {
    public void init(IMutableValueStorage state, IValueReference defaultValue) throws IOException;

    public void step(IMutableValueStorage state, IValueReference value) throws IOException;

    // TODO: Second param was initially an IPointable.
    public void finish(IMutableValueStorage state, DataOutput out) throws IOException;
}
