/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.api;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IStreamDataParser extends IDataParser {
    /**
     * Sets the inputStream for the parser. called only for parsers that support InputStreams
     *
     * @throws IOException
     */
    public void setInputStream(InputStream in) throws IOException;

    /**
     * Parse data into output AsterixDataModel binary records.
     * Used with parsers that support stream sources
     *
     * @param out
     *            DataOutput instance that for writing the parser output.
     * @throws IOException
     */
    public boolean parse(DataOutput out) throws HyracksDataException;

    /**
     * reset the parser state. this is called when a failure takes place
     * and the job needs to continue and to do that, the parser need to
     * be in a consistent state
     *
     * @return true if reset was successful, false, otherwise
     * @throws IOException
     */
    public boolean reset(InputStream in) throws IOException;
}
