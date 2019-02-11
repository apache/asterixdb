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
package org.apache.asterix.common.api;

import java.util.Map;

public interface ICommonRequestParameters {

    /**
     * The request reference of this {@link ICommonRequestParameters}
     *
     * @return the request reference
     */
    IRequestReference getRequestReference();

    /**
     * @return the client context id for the request
     */
    String getClientContextId();

    /**
     * @return Optional request parameters. Otherwise null.
     */
    Map<String, String> getOptionalParameters();

    /**
     * @return true if the request accepts multiple statements. Otherwise, false.
     */
    boolean isMultiStatement();

    /**
     * Gets the statement the client provided with the request
     *
     * @return the request statement
     */
    String getStatement();
}
