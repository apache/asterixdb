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
package org.apache.asterix.common.ioopcallbacks;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;

/**
 * Told of every flush and merge that completes successfully, for metrics. Invoked on the IO operation's thread, so it
 * must not block.
 */
@FunctionalInterface
public interface ILSMIOOperationListener {
    ILSMIOOperationListener NO_OP = (type, durationNanos, componentBytes) -> {
    };

    /**
     * @param type {@link LSMIOOperationType#FLUSH} or {@link LSMIOOperationType#MERGE}
     * @param durationNanos how long the operation ran, from its start until its new component was durable and
     *            checkpointed; time spent queued, or waiting to enter the index, is not included
     * @param componentBytes the on-disk size of the component the operation wrote
     */
    void completed(LSMIOOperationType type, long durationNanos, long componentBytes);
}
