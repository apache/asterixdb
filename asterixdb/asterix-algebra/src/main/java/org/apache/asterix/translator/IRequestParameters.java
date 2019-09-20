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

import org.apache.asterix.common.api.ICommonRequestParameters;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.api.result.IResultSet;

public interface IRequestParameters extends ICommonRequestParameters {

    /**
     * @return A Resultset client object that is used to read the results.
     */
    IResultSet getResultSet();

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
     * @return Statement parameters
     */
    Map<String, IAObject> getStatementParameters();

    /**
     * @return a bitmask that restricts which statement
     *   {@link org.apache.asterix.lang.common.base.Statement.Category categories} are permitted for this request,
     *   {@code 0} if all categories are allowed
     */
    int getStatementCategoryRestrictionMask();
}
