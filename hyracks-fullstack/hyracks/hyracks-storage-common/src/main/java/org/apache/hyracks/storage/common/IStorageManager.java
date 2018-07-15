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

import java.io.Serializable;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;

/**
 * Provides storage components during hyracks tasks execution
 */
public interface IStorageManager extends Serializable, IJsonSerializable {
    /**
     * @param ctx
     *            the nc service context
     * @return the disk buffer cache {@link org.apache.hyracks.storage.common.buffercache.IBufferCache}
     */
    IBufferCache getBufferCache(INCServiceContext ctx);

    /**
     * @param ctx
     *            the nc service context
     * @return the local resource repository {@link org.apache.hyracks.storage.am.common.build.ILocalResourceRepository}
     */
    ILocalResourceRepository getLocalResourceRepository(INCServiceContext ctx);

    /**
     * @param ctx
     *            the nc service context
     * @return the resource id factory {@link org.apache.hyracks.storage.common.file.IResourceIdFactory}
     */
    IResourceIdFactory getResourceIdFactory(INCServiceContext ctx);

    /**
     * @param ctx
     *            the nc service context
     * @return the resource lifecycle manager
     */
    IResourceLifecycleManager<IIndex> getLifecycleManager(INCServiceContext ctx);
}
