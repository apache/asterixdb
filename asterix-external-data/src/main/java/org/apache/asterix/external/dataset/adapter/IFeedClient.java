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
package org.apache.asterix.external.dataset.adapter;

import java.io.DataOutput;

import org.apache.asterix.common.exceptions.AsterixException;

public interface IFeedClient {

    public enum InflowState {
        NO_MORE_DATA,
        DATA_AVAILABLE,
        DATA_NOT_AVAILABLE
    }

    /**
     * Writes the next fetched tuple into the provided instance of DatatOutput. Invocation of this method blocks until
     * a new tuple has been written or the specified time has expired.
     * 
     * @param dataOutput
     *            The receiving channel for the feed client to write ADM records to.
     * @param timeout
     *            Threshold time (expressed in seconds) for the next tuple to be obtained from the external source.
     * @return
     * @throws AsterixException
     */
    public InflowState nextTuple(DataOutput dataOutput, int timeout) throws AsterixException;

}
