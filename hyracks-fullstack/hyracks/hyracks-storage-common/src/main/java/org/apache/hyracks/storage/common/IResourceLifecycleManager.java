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
package org.apache.hyracks.storage.common;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The base interface for resource management
 *
 * @param <R>
 *            resource class
 */
public interface IResourceLifecycleManager<R> extends IResourceMemoryManager {
    /**
     * get a list of all resources which are opened
     *
     * @return
     */
    public List<R> getOpenResources();

    /**
     * Registers a resource.
     *
     * @param resourceId
     * @param resource
     * @throws HyracksDataException
     *             if a resource is already registered with this resourceId
     */
    public void register(String resourceId, R resource) throws HyracksDataException;

    /**
     * Opens a resource. The resource is moved to the open state
     *
     * @param resourceId
     * @throws HyracksDataException
     *             if no resource with the resourceId exists
     */
    public void open(String resourceId) throws HyracksDataException;

    /**
     * closes a resource and free up its allocated resources
     *
     * @param resourceId
     * @throws HyracksDataException
     */
    public void close(String resourceId) throws HyracksDataException;

    /**
     * gets the resource registered with this id
     *
     * @param resourceId
     * @return the resource if registered or null
     * @throws HyracksDataException
     */
    public R get(String resourceId) throws HyracksDataException;

    /**
     * unregister a resource removing its resources in memory and on disk
     *
     * @param resourceId
     * @throws HyracksDataException
     */
    public void unregister(String resourceId) throws HyracksDataException;
}
