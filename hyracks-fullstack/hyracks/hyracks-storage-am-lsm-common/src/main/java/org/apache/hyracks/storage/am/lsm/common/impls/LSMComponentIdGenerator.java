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

    public LSMComponentIdGenerator(int numComponents, long lastUsedId) {
        this.numComponents = numComponents;
        this.lastUsedId = lastUsedId;
        refresh();
        currentComponentIndex = 0;
    }

    @Override
    public synchronized void refresh() {
        final long nextId = ++lastUsedId;
        componentId = new LSMComponentId(nextId, nextId);
        currentComponentIndex = (currentComponentIndex + 1) % numComponents;
    }

    @Override
    public synchronized ILSMComponentId getId() {
        return componentId;
    }

    @Override
    public synchronized int getCurrentComponentIndex() {
        return currentComponentIndex;
    }
}
