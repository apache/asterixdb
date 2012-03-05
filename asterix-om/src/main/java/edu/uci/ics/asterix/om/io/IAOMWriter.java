package edu.uci.ics.asterix.om.io;

import edu.uci.ics.asterix.om.base.IAObject;

public interface IAOMWriter {

    // Initializes a writer for the collection defined by a certain location.
    public void init(IALocation location) throws AsterixIOException;

    // Appends the object to the previously opened collection.
    public void append(IAObject object) throws AsterixIOException;

    // Closes the writer, deallocates any additional structures.
    public void close() throws AsterixIOException;

}
