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
package org.apache.asterix.cloud.clients.profiler.limiter;

/**
 * Rate limiter for Cloud request. If the number of requests per seconds exceeds the provided limit, then
 * the requester threads will be throttled.
 */
public interface IRequestRateLimiter {
    /**
     * Perform a write request
     */
    void writeRequest();

    /**
     * Perform a read request
     */
    void readRequest();

    /**
     * Perform a list request
     */
    void listRequest();

    /**
     * Get the number of throttled read requests
     *
     * @return the number of throttled read requests
     */
    long getReadThrottleCount();

    /**
     * Get the number of throttled write requests
     *
     * @return the number of throttled write requests
     */
    long getWriteThrottleCount();
}
