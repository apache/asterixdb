package edu.uci.ics.asterix.om.io;

import edu.uci.ics.asterix.om.base.IAObject;

public interface IAOMReader {

    // Initializes a reader for the collection defined by a certain location.
    public void init(IALocation location) throws AsterixIOException;

    // Reads the current object and goes to the next item in the collection.
    // When it reaches the end, it returns null.
    public IAObject read() throws AsterixIOException;

    public void close() throws AsterixIOException;
}
