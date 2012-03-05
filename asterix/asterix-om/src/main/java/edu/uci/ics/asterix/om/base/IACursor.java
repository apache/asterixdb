/**
 * 
 */
package edu.uci.ics.asterix.om.base;

/**
 * @author Nicola
 *         Implements a standard Cursor interface: initially, the cursor points
 *         before the beginning of the data. The first call to next() moves the
 *         cursor to the first object, if any. Subsequent calls keep moving the
 *         cursor, until next() returns false.
 */
public interface IACursor {
    public void reset();

    public boolean next(); // moves the cursor

    public IAObject get(); // returns the current object
}
