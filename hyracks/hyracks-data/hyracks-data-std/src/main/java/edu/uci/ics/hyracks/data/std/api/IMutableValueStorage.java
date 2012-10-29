package edu.uci.ics.hyracks.data.std.api;

public interface IMutableValueStorage extends IValueReference, IDataOutputProvider {
    public void reset();
}