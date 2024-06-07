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

/**
 * A proxy to notify a disk-cache (a faster disk that is caching a slower resource) about resource lifecycle events.
 * The notifier could block a resource from being operated on if the disk-cache manager denying access to a resource
 */
public interface IDiskResourceCacheLockNotifier {
    /**
     * Notify registering a new resource
     * Note: this method is not thread-safe outside {@link org.apache.hyracks.storage.common.IResourceLifecycleManager}
     *
     * @param localResource resource to be registered
     * @param index         of the resource
     */
    void onRegister(LocalResource localResource, IIndex index);

    /**
     * Notify unregistering an existing resource
     * Note: this method is not thread-safe outside {@link org.apache.hyracks.storage.common.IResourceLifecycleManager}
     *
     * @param resourceId resource ID
     */
    void onUnregister(long resourceId);

    /**
     * Notify opening a resource
     *
     * @param resourceId resource ID
     */
    void onOpen(long resourceId);

    /**
     * Notify closing a resource
     *
     * @param resourceId resource ID
     */
    void onClose(long resourceId);
}
