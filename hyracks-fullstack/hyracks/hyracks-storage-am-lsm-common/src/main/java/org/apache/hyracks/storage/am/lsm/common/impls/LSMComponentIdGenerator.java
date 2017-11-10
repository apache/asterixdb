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

package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;

/**
 * A default implementation of {@link ILSMComponentIdGenerator}.
 *
 */
public class LSMComponentIdGenerator implements ILSMComponentIdGenerator {

    protected long previousTimestamp = -1L;

    private ILSMComponentId componentId;

    public LSMComponentIdGenerator() {
        refresh();
    }

    @Override
    public void refresh() {
        long ts = getCurrentTimestamp();
        componentId = new LSMComponentId(ts, ts);
    }

    @Override
    public ILSMComponentId getId() {
        return componentId;
    }

    protected long getCurrentTimestamp() {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= previousTimestamp) {
            // make sure timestamp is strictly increasing
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            timestamp = System.currentTimeMillis();
        }
        previousTimestamp = timestamp;
        return timestamp;

    }

}
