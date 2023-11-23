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
package org.apache.asterix.common.context;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Info {

    private static final Logger LOGGER = LogManager.getLogger();

    private final AtomicInteger referenceCount = new AtomicInteger();
    private volatile boolean isOpen;

    public Info() {
        isOpen = false;
    }

    public void touch() {
        referenceCount.incrementAndGet();
    }

    public void untouch() {
        int currentRefCount = referenceCount.get();
        if (currentRefCount <= 0) {
            LOGGER.warn("trying to decrement ref count {} that is already <=0", currentRefCount);
        }
        referenceCount.updateAndGet(i -> i > 0 ? i - 1 : i);
    }

    public int getReferenceCount() {
        return referenceCount.get();
    }

    public boolean isOpen() {
        return isOpen;
    }

    public void setOpen(boolean isOpen) {
        this.isOpen = isOpen;
    }
}
