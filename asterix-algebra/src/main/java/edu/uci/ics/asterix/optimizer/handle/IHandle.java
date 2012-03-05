package edu.uci.ics.asterix.optimizer.handle;

/**
 * 
 * A handle is a way of accessing an ADM instance or a collection of ADM
 * instances nested within another ADM instance.
 * 
 * @author Nicola
 * 
 */

public interface IHandle {
    public enum HandleType {
        FIELD_INDEX_AND_TYPE, FIELD_NAME
    }

    public HandleType getHandleType();
}
