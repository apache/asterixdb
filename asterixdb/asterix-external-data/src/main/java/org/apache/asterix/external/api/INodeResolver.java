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

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;

/**
 * A policy for resolving a name to a node controller id.
 */
@FunctionalInterface
public interface INodeResolver {

    /**
     * Resolve a passed-in value to a node controller id.
     *
     * @param value
     *            string to be resolved
     * @return resolved result (a node controller id)
     * @throws AsterixException
     */
    String resolveNode(ICcApplicationContext appCtx, String value, Map<InetAddress, Set<String>> ncMap, Set<String> ncs)
            throws AsterixException;
}
