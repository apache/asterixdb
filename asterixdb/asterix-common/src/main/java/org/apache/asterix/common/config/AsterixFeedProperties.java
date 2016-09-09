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
package org.apache.asterix.common.config;

import org.apache.hyracks.util.StorageUtil;

import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

public class AsterixFeedProperties extends AbstractAsterixProperties {

    private static final String FEED_CENTRAL_MANAGER_PORT_KEY = "feed.central.manager.port";
    private static final int FEED_CENTRAL_MANAGER_PORT_DEFAULT = 4500;

    private static final String FEED_MEMORY_GLOBALBUDGET_KEY = "feed.memory.global.budget";
    private static final long FEED_MEMORY_GLOBALBUDGET_DEFAULT = StorageUtil.getSizeInBytes(64, MEGABYTE);
                                                                 // i.e. 2048 frames (assuming 32768 as frame size)

    private static final String FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_KEY = "feed.memory.available.wait.timeout";
    private static final long FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_DEFAULT = 10; // 10 seconds

    private static final String FEED_PENDING_WORK_THRESHOLD_KEY = "feed.pending.work.threshold";
    private static final int FEED_PENDING_WORK_THRESHOLD_DEFAULT = 50;

    private static final String FEED_MAX_SUCCESSIVE_THRESHOLD_PERIOD_KEY = "feed.max.threshold.period";
    private static final int FEED_MAX_SUCCESSIVE_THRESHOLD_PERIOD_DEFAULT = 5;

    public AsterixFeedProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    @PropertyKey(FEED_MEMORY_GLOBALBUDGET_KEY)
    public long getMemoryComponentGlobalBudget() {
        return accessor.getProperty(FEED_MEMORY_GLOBALBUDGET_KEY, FEED_MEMORY_GLOBALBUDGET_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    @PropertyKey(FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_KEY)
    public long getMemoryAvailableWaitTimeout() {
        return accessor.getProperty(FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_KEY, FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    /**
     * @return port at which the Central Feed Manager listens for control messages from local Feed Managers
     */
    @PropertyKey(FEED_CENTRAL_MANAGER_PORT_KEY)
    public int getFeedCentralManagerPort() {
        return accessor.getProperty(FEED_CENTRAL_MANAGER_PORT_KEY, FEED_CENTRAL_MANAGER_PORT_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    /**
     * @return maximum length of input queue before triggering corrective action
     */
    @PropertyKey(FEED_PENDING_WORK_THRESHOLD_KEY)
    public int getPendingWorkThreshold() {
        return accessor.getProperty(FEED_PENDING_WORK_THRESHOLD_KEY, FEED_PENDING_WORK_THRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(FEED_MAX_SUCCESSIVE_THRESHOLD_PERIOD_KEY)
    public int getMaxSuccessiveThresholdPeriod() {
        return accessor.getProperty(FEED_MAX_SUCCESSIVE_THRESHOLD_PERIOD_KEY,
                FEED_MAX_SUCCESSIVE_THRESHOLD_PERIOD_DEFAULT, PropertyInterpreters.getIntegerPropertyInterpreter());
    }
}
