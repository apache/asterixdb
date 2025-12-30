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
package org.apache.asterix.external.util.aws;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * We are implementing this registry class to ensure that the created AWS clients are closed properly for the
 * following scenario:
 * <p>
 * When using assume role authentication, aside from Glue/S3 clients, we are also creating STS client and AssumeRoleCredentialsProvider
 * which require manual closing when done. The Iceberg default behavior closes only the used clients, that is, S3 and Glue client,
 * and the other resources are leaked.
 * <p>
 * To deal with this, we are keeping track of all the created clients (per request), and upon closing the catalog, we are also
 * closing all the open resources.
 * <p>
 * This is applicable to any clients that suffer from the same issue.
 */
public final class EnsureCloseClientsFactoryRegistry {

    public static final String FACTORY_INSTANCE_ID_KEY = "ensureClose.factory.instanceId";

    private static final ConcurrentHashMap<String, Set<AutoCloseable>> REGISTRY = new ConcurrentHashMap<>();

    private EnsureCloseClientsFactoryRegistry() {
    }

    public static void register(String instanceId, AutoCloseable factory) {
        if (instanceId == null || factory == null) {
            return;
        }
        REGISTRY.computeIfAbsent(instanceId, k -> ConcurrentHashMap.newKeySet()).add(factory);
    }

    public static void unregister(String instanceId, AutoCloseable factory) {
        if (instanceId == null || factory == null) {
            return;
        }
        Set<AutoCloseable> set = REGISTRY.get(instanceId);
        if (set == null) {
            return;
        }
        set.remove(factory);
        if (set.isEmpty()) {
            REGISTRY.remove(instanceId, set);
        }
    }

    public static void closeAll(String instanceId) {
        if (instanceId == null) {
            return;
        }

        Set<AutoCloseable> set = REGISTRY.remove(instanceId);
        if (set == null || set.isEmpty()) {
            return;
        }

        RuntimeException first = null;
        for (AutoCloseable c : set) {
            try {
                c.close();
            } catch (Exception e) {
                if (first == null) {
                    first = new RuntimeException(e);
                } else {
                    first.addSuppressed(e);
                }
            }
        }

        if (first != null) {
            throw first;
        }
    }
}