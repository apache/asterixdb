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
import org.apache.hyracks.util.annotations.ThreadSafe;

/**
 * A default implementation of {@link ILSMComponentIdGenerator}.
 */
@ThreadSafe
public class LSMComponentIdGenerator implements ILSMComponentIdGenerator {

    private final int numComponents;
    private int currentComponentIndex;
    private long lastUsedId;
    private ILSMComponentId componentId;
    private boolean initialized = false;

    public LSMComponentIdGenerator(int numComponents) {
        this.numComponents = numComponents;
    }

    @Override
    public synchronized void init(long lastUsedId) {
        this.lastUsedId = lastUsedId;
        initialized = true;
        refresh();
        currentComponentIndex = 0;
    }

    @Override
    public synchronized void refresh() {
        if (!initialized) {
            throw new IllegalStateException("Attempt to refresh component id before initialziation.");
        }
        final long nextId = ++lastUsedId;
        componentId = new LSMComponentId(nextId, nextId);
        currentComponentIndex = (currentComponentIndex + 1) % numComponents;
    }

    @Override
    public synchronized ILSMComponentId getId() {
        if (!initialized) {
            throw new IllegalStateException("Attempt to get component id before initialziation.");
        }
        return componentId;
    }

    @Override
    public synchronized int getCurrentComponentIndex() {
        return currentComponentIndex;
    }
}
