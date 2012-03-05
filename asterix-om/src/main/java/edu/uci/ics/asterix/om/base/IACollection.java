package edu.uci.ics.asterix.om.base;

public interface IACollection extends IAObject {
    /** may be expensive for certain list implementations */
    public int size();

    public IACursor getCursor();
}
