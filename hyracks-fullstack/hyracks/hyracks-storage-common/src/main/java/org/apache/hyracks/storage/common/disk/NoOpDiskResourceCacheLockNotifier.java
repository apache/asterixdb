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
package org.apache.hyracks.storage.common.disk;

import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.LocalResource;

public final class NoOpDiskResourceCacheLockNotifier implements IDiskResourceCacheLockNotifier {
    public static final IDiskResourceCacheLockNotifier INSTANCE = new NoOpDiskResourceCacheLockNotifier();

    private NoOpDiskResourceCacheLockNotifier() {
    }

    @Override
    public void onRegister(LocalResource localResource, IIndex index) {
        // NoOp
    }

    @Override
    public void onUnregister(long resourceId) {
        // NoOp
    }

    @Override
    public void onOpen(long resourceId) {
        // NoOp
    }

    @Override
    public void onClose(long resourceId) {
        // NoOp
    }
}
