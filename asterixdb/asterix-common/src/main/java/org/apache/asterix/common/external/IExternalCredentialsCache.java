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

public interface IExternalCredentialsCache {

    /**
     * Returns the cached credentials.
     *
     * @param key credentials key
     * @return credentials if present and not expired/need refreshing, null otherwise
     */
    Object get(String key);

    /**
     * Deletes the cache for the provided entity
     *
     * @param key credentials key
     */
    void delete(String key);

    /**
     * Updates the credentials cache with the provided credentials for the specified name
     *
     * @param key credentials key
     * @param credentials credentials to cache
     */
    void put(String key, Object credentials);
}
