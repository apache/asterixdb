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
package org.apache.asterix.translator;

import java.util.Map;

import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.api.dataset.IHyracksDataset;

public interface IRequestParameters {

    /**
     * @return A Hyracks dataset client object that is used to read the results.
     */
    IHyracksDataset getHyracksDataset();

    /**
     * Gets the required result properties of the request.
     *
     * @return the result properties
     */
    ResultProperties getResultProperties();

    /**
     * @return a reference to write the stats of executed queries
     */
    Stats getStats();

    /**
     * @return a reference to write the metadata of executed queries
     */
    IStatementExecutor.ResultMetadata getOutMetadata();

    /**
     * @return the client context id for the query
     */
    String getClientContextId();

    /**
     * @return Optional request parameters. Otherwise null.
     */
    Map<String, String> getOptionalParameters();

    /**
     * @return Statement parameters
     */
    Map<String, IAObject> getStatementParameters();

    /**
     * @return true if the request accepts multiple statements. Otherwise, false.
     */
    boolean isMultiStatement();
}
