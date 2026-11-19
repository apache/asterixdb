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
package org.apache.asterix.cloud.clients.metrics;

/**
 * Told of every request a cloud client completes, for metrics. Invoked on the thread completing the request, which may
 * be an I/O event loop thread, so it must not block.
 */
@FunctionalInterface
public interface ICloudRequestListener {
    ICloudRequestListener NO_OP = (operation, durationNanos, failure) -> {
    };

    /**
     * @param operation the kind of request
     * @param durationNanos how long the request took, including any retries the client made
     * @param failure why the request failed, or {@code null} if it succeeded
     */
    void requestCompleted(CloudRequestOperation operation, long durationNanos, CloudRequestFailure failure);
}
