package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.DataOutput;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IPullBasedFeedClient {

    public enum InflowState {
        NO_MORE_DATA,
        DATA_AVAILABLE,
        DATA_NOT_AVAILABLE
    }

    /**
     * Writes the next fetched tuple into the provided instance of DatatOutput.
     * 
     * @param dataOutput
     *            The receiving channel for the feed client to write ADM records to.
     * @return true if a record was written to the DataOutput instance
     *         false if no record was written to the DataOutput instance indicating non-availability of new data.
     * @throws AsterixException
     */
    public InflowState nextTuple(DataOutput dataOutput) throws AsterixException;

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
     * @param configuration
     */
    public boolean alter(Map<String, String> configuration);

}
