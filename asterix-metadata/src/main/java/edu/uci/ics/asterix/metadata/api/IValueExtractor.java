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

package edu.uci.ics.asterix.metadata.api;

import java.io.IOException;

import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Extracts an object of type T from a given tuple. Implementations of this
 * interface are used for extracting specific fields of metadata records within
 * the metadata node. The intention is to have a single transactional search
 * procedure than can deliver different objects based on the type of value
 * extractor passed to the search procedure.
 */
public interface IValueExtractor<T> {
    /**
     * Extracts an object of type T from a given tuple.
     * 
     * @param jobId
     *            A globally unique transaction id.
     * @param tuple
     *            Tuple from which an object shall be extracted.
     * @return New object of type T.
     * @throws MetadataException
     * @throws HyracksDataException
     * @throws IOException
     */
    public T getValue(JobId jobId, ITupleReference tuple) throws MetadataException, HyracksDataException, IOException;
}
