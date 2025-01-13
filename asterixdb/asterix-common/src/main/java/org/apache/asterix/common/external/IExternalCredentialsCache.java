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
package org.apache.asterix.common.external;

import java.util.Map;

public interface IExternalCredentialsCache {

    /**
     * Returns the cached credentials. Can be of any supported external credentials type
     *
     * @param configuration configuration containing external collection details
     * @return credentials if present, null otherwise
     */
    Object getCredentials(Map<String, String> configuration);

    /**
     * Updates the credentials cache with the provided credentials for the specified name
     *
     * @param configuration configuration containing external collection details
     * @param credentials credentials to cache
     */
    void updateCache(Map<String, String> configuration, Map<String, String> credentials);

    /**
     * Deletes the cache for the provided entity name
     *
     * @param name name of the entity for which the credentials are to be deleted
     */
    void deleteCredentials(String name);

    /**
     * Returns the name of the entity which the cached credentials belong to
     *
     * @param configuration configuration containing external collection details
     * @return name of entity which credentials belong to
     */
    String getName(Map<String, String> configuration);
}
