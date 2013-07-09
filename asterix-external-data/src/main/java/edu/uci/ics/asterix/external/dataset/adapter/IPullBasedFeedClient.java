/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
