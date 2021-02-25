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
package org.apache.asterix.common.transactions;

public interface IResourceIdManager {

    /**
     * @return the created resource id, or <code>-1</code> if a resource cannot be created
     */
    long createResourceId();

    boolean reported(String nodeId);

    void report(String nodeId, long maxResourceId);

    /**
     * @param blockSize the size of resource id block to create
     * @return the starting id of contiguous block of resource ids, or <code>-1</code> if
     *         the resource block cannot be created
     */
    long createResourceIdBlock(int blockSize);
}
