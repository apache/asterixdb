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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.metadata.Namespace;

/**
 * Identifies a cached plan: the engine's plan-affecting request inputs.
 *
 * @param queryString
 * @param optimize
 * @param config
 * @param maxWarnings
 * @param resultSetId
 * @param defaultNamespace
 */
public record QueryPlanCacheKey(String queryString, boolean optimize, Map<String, Object> config, long maxWarnings,
        long resultSetId, Namespace defaultNamespace) implements IQueryPlanCacheKey {

    public QueryPlanCacheKey {
        // defensive copy so the key stays stable if the source map is later mutated
        config = Collections.unmodifiableMap(new HashMap<>(config));
    }
}
