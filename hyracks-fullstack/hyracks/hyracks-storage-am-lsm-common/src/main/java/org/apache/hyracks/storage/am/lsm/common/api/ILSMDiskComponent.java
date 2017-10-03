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
package org.apache.hyracks.storage.am.lsm.common.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;

public interface ILSMDiskComponent extends ILSMComponent {

    @Override
    default LSMComponentType getType() {
        return LSMComponentType.DISK;
    }

    @Override
    DiskComponentMetadata getMetadata();

    /**
     * @return the on disk size of this component
     */
    long getComponentSize();

    /**
     * @return the reference count for the component
     */
    int getFileReferenceCount();

    /**
     * Delete the component from disk
     *
     * @throws HyracksDataException
     */
    void destroy() throws HyracksDataException;

    /**
     * Return the component Id of this disk component from its metadata
     *
     * @return
     * @throws HyracksDataException
     */
    ILSMDiskComponentId getComponentId() throws HyracksDataException;

    /**
     * Mark the component as valid
     *
     * @param persist
     *            whether the call should force data to disk before returning
     * @throws HyracksDataException
     */
    void markAsValid(boolean persist) throws HyracksDataException;

}
