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
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.MemoryComponentMetadata;

public interface ILSMMemoryComponent extends ILSMComponent {
    @Override
    default LSMComponentType getType() {
        return LSMComponentType.MEMORY;
    }

    @Override
    MemoryComponentMetadata getMetadata();

    /**
     * @return true if the component can be entered for reading
     */
    boolean isReadable();

    /**
     * @return the number of writers inside the component
     */
    int getWriterCount();

    /**
     * Reset the memory component's state after the flush completes
     *
     * @throws HyracksDataException
     */
    void reset() throws HyracksDataException;

    /**
     * Cleanup the memory component after flush (can be time consuming)
     *
     * @throws HyracksDataException
     */
    void cleanup() throws HyracksDataException;

    /**
     * @return true if the memory component has been modified since it was last reset, whether by tuples or by its
     *         metadata alone, false otherwise
     */
    boolean isModified();

    /**
     * Set the component as modified
     */
    void setModified();

    /**
     * @return true if tuples have been written to the memory component since it was last reset, false if it is
     *         unmodified or its only modifications are to its metadata
     */
    boolean hasTuples();

    /**
     * Set the component as holding tuples, and so as {@link #setModified() modified}
     */
    void setHasTuples();

    /**
     * Makes this component known to the memory budget without taking any of it. Pages are taken separately, by
     * {@link #allocate()} or on the component's first activation.
     *
     * @throws HyracksDataException
     */
    void register() throws HyracksDataException;

    /**
     * Allocates memory to this component, create and activate it.
     * This method is atomic. If an exception is thrown, then the call had no effect.
     *
     * @throws HyracksDataException
     */
    void allocate() throws HyracksDataException;

    /**
     * Deactivete the memory component, destroy it, and deallocates its memory
     *
     * @throws HyracksDataException
     */
    void deallocate() throws HyracksDataException;

    /**
     * Test method
     * TODO: Get rid of it
     *
     * @throws HyracksDataException
     */
    void validate() throws HyracksDataException;

    /**
     * Reset the component Id of the memory component after it's recycled
     *
     * @param newId
     * @param force
     *            Whether to force reset the Id to skip sanity checks
     * @throws HyracksDataException
     */
    void resetId(ILSMComponentId newId, boolean force) throws HyracksDataException;

    /**
     * Set the component state to be unwritable to prevent future writers from non-force
     * entry to the component
     */
    void setUnwritable();

    /**
     *
     * @return the file references of the component
     */
    LSMComponentFileReferences getComponentFileRefs();

    /**
     * Called when the memory component is flushed to disk
     *
     * @throws HyracksDataException
     */
    void flushed() throws HyracksDataException;

}
