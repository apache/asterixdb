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
package edu.uci.ics.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;

/**
 * Interface implemented by a parser
 */
public interface IDataParser {

    /**
     * Initialize the parser prior to actual parsing.
     * 
     * @param in
     *            input stream to be parsed
     * @param recordType
     *            record type associated with input data
     * @param datasetRec
     *            boolean flag set to true if input data represents dataset
     *            records.
     * @throws AsterixException
     * @throws IOException
     */
    public void initialize(InputStream in, ARecordType recordType, boolean datasetRec) throws AsterixException,
            IOException;

    /**
     * Parse data from source input stream and output ADM records.
     * 
     * @param out
     *            DataOutput instance that for writing the parser output.
     * @return
     * @throws AsterixException
     * @throws IOException
     */
    public boolean parse(DataOutput out) throws AsterixException, IOException;
}
