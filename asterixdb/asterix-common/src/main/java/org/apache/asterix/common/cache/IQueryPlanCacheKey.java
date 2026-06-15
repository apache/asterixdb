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
package org.apache.asterix.common.cache;

import java.util.Map;

import org.apache.asterix.common.metadata.Namespace;

/**
 * Identifies a cached compiled plan in the {@link IQueryPlanCache}. Two requests share a plan only when all of these
 * plan-affecting inputs are equal.
 */
public interface IQueryPlanCacheKey {

    /**
     * @return the raw request text this key identifies
     */
    String queryString();

    /**
     * @return whether optimization runs for this request
     */
    boolean optimize();

    /**
     * @return the request's full config map
     */
    Map<String, Object> config();

    /**
     * @return the request's max-warnings limit
     */
    long maxWarnings();

    /**
     * @return the result set id, disambiguating statements in a multi-statement request
     */
    long resultSetId();

    /**
     * @return the namespace that unqualified names resolve against
     */
    Namespace defaultNamespace();
}
