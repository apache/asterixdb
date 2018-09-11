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
package org.apache.asterix.active;

import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IActiveRuntime {

    /**
     * @return the unique runtime id associated with the active runtime
     */
    ActiveRuntimeId getRuntimeId();

    /**
     * Stops the running activity
     *
     * @param timeout
     *            time for graceful stop. interrupt the runtime after that
     * @param unit
     *            unit of the timeout
     *
     * @throws HyracksDataException
     * @throws InterruptedException
     */
    void stop(long timeout, TimeUnit unit) throws HyracksDataException, InterruptedException;

    /**
     * @return the runtime stats for monitoring purposes
     */
    default String getStats() {
        return "\"Runtime stats is not available.\"";
    }
}
