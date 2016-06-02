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
package org.apache.hyracks.storage.common.buffercache;

/**
 * Allows customization of the page cleaning strategy by the cleaner thread.
 *
 * @author vinayakb
 */
public interface IPageCleanerPolicy {
    /**
     * Callback from the cleaner just before the beginning of a cleaning cycle.
     *
     * @param monitor
     *            - The monitor on which a mutex is held while in this call
     */
    void notifyCleanCycleStart(Object monitor) throws InterruptedException;

    /**
     * Callback from the cleaner just after the finish of a cleaning cycle.
     *
     * @param monitor
     *            - The monitor on which a mutex is held while in this call.
     */
    void notifyCleanCycleFinish(Object monitor) throws InterruptedException;

    /**
     * Callback to indicate that no victim was found.
     *
     * @param monitor
     *            - The monitor on which a mutex is held while in this call.
     */
    void notifyVictimNotFound(Object monitor) throws InterruptedException;
}
