package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IPullBasedFeedClient {

    /**
     * Writes the next fetched tuple into the provided instance of DatatOutput.
     * 
     * @param dataOutput
     *            The receiving channel for the feed client to write ADM records to.
     * @return true if a record was written to the DataOutput instance
     *         false if no record was written to the DataOutput instance indicating non-availability of new data.
     * @throws AsterixException
     */
    public boolean nextTuple(DataOutput dataOutput) throws AsterixException;

    /**
     * Provides logic for any corrective action that feed client needs to execute on
     * encountering an exception.
     * 
     * @param e
     *            The exception encountered during fetching of data from external source
     * @throws AsterixException
     */
    public void resetOnFailure(Exception e) throws AsterixException;

    /**
     * Terminates a feed, that is data ingestion activity ceases.
     * 
     * @throws Exception
     */
    public void stop() throws Exception;

}
