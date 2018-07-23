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
package org.apache.hyracks.storage.am.lsm.common.util;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ComponentUtils {

    private static final Logger LOGGER = LogManager.getLogger();
    public static final MutableArrayValueReference MARKER_LSN_KEY = new MutableArrayValueReference("Marker".getBytes());
    public static final long NOT_FOUND = -1L;

    private ComponentUtils() {
    }

    /**
     * Get a long value from the metadata of a component or return a default value
     *
     * @param metadata
     *            the component's metadata
     * @param key
     *            the key
     * @param defaultValue
     *            the default value
     * @return
     *         the long value if found, the default value otherwise
     * @throws HyracksDataException
     *             If the comopnent was a disk component and an IO error was encountered
     */
    public static long getLong(IComponentMetadata metadata, IValueReference key, long defaultValue,
            ArrayBackedValueStorage value) throws HyracksDataException {
        metadata.get(key, value);
        return value.getLength() == 0 ? defaultValue
                : LongPointable.getLong(value.getByteArray(), value.getStartOffset());
    }

    /**
     * Get a value from an index's metadata pages. It first, searches the current in memory component
     * then searches the other components. in reverse order.
     * Note: This method locks on the OpTracker of the index
     *
     * @param index
     * @param key
     * @param value
     * @throws HyracksDataException
     */
    public static void get(ILSMIndex index, IValueReference key, ArrayBackedValueStorage value)
            throws HyracksDataException {
        boolean loggable = LOGGER.isDebugEnabled();
        value.reset();
        if (loggable) {
            LOGGER.log(Level.DEBUG, "Getting " + key + " from index " + index);
        }
        // Lock the opTracker to ensure index components don't change
        synchronized (index.getOperationTracker()) {
            ILSMMemoryComponent cmc = index.getCurrentMemoryComponent();
            if (cmc.isReadable()) {
                index.getCurrentMemoryComponent().getMetadata().get(key, value);
            }
            if (value.getLength() == 0) {
                if (loggable) {
                    LOGGER.log(Level.DEBUG, key + " was not found in mutable memory component of " + index);
                }
                // was not found in the in current mutable component, search in the other in memory components
                fromImmutableMemoryComponents(index, key, value);
                if (value.getLength() == 0) {
                    if (loggable) {
                        LOGGER.log(Level.DEBUG, key + " was not found in all immmutable memory components of " + index);
                    }
                    // was not found in the in all in memory components, search in the disk components
                    fromDiskComponents(index, key, value);
                    if (loggable) {
                        if (value.getLength() == 0) {
                            LOGGER.log(Level.DEBUG, key + " was not found in all disk components of " + index);
                        } else {
                            LOGGER.log(Level.DEBUG, key + " was found in disk components of " + index);
                        }
                    }
                } else {
                    if (loggable) {
                        LOGGER.log(Level.DEBUG, key + " was found in the immutable memory components of " + index);
                    }
                }
            } else {
                if (loggable) {
                    LOGGER.log(Level.DEBUG, key + " was found in mutable memory component of " + index);
                }
            }
        }
    }

    /**
     * Put LSM metadata state into the index's current memory component.
     *
     * @param index,
     *            the LSM index.
     * @param key,
     *            the key for the metadata state.
     * @param pointable,
     *            the value for the metadata state.
     * @throws HyracksDataException
     */
    public static void put(ILSMIndex index, IValueReference key, IPointable pointable) throws HyracksDataException {
        // write the opTracker to ensure the component layout don't change
        synchronized (index.getOperationTracker()) {
            index.getCurrentMemoryComponent().getMetadata().put(key, pointable);
        }
    }

    private static void fromDiskComponents(ILSMIndex index, IValueReference key, ArrayBackedValueStorage value)
            throws HyracksDataException {
        boolean loggable = LOGGER.isDebugEnabled();
        if (loggable) {
            LOGGER.log(Level.DEBUG, "Getting " + key + " from disk components of " + index);
        }
        for (ILSMDiskComponent c : index.getDiskComponents()) {
            if (loggable) {
                LOGGER.log(Level.DEBUG, "Getting " + key + " from disk components " + c);
            }
            c.getMetadata().get(key, value);
            if (value.getLength() != 0) {
                // Found
                return;
            }
        }
    }

    private static void fromImmutableMemoryComponents(ILSMIndex index, IValueReference key,
            ArrayBackedValueStorage value) throws HyracksDataException {
        boolean loggable = LOGGER.isDebugEnabled();
        if (loggable) {
            LOGGER.log(Level.DEBUG, "Getting " + key + " from immutable memory components of " + index);
        }
        List<ILSMMemoryComponent> memComponents = index.getMemoryComponents();
        int numOtherMemComponents = memComponents.size() - 1;
        int next = index.getCurrentMemoryComponentIndex();
        if (loggable) {
            LOGGER.log(Level.DEBUG, index + " has " + numOtherMemComponents + " immutable memory components");
        }
        for (int i = 0; i < numOtherMemComponents; i++) {
            if (loggable) {
                LOGGER.log(Level.DEBUG,
                        "trying to get " + key + " from immutable memory components number: " + (i + 1));
            }
            next = next - 1;
            if (next < 0) {
                next = memComponents.size() - 1;
            }
            ILSMMemoryComponent c = index.getMemoryComponents().get(next);
            if (c.isReadable()) {
                c.getMetadata().get(key, value);
                if (value.getLength() != 0) {
                    // Found
                    return;
                }
            }
        }
    }

    public static void markAsValid(ITreeIndex treeIndex, boolean forceToDisk, IPageWriteFailureCallback callback)
            throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        treeIndex.getPageManager().close(callback);
        if (callback.hasFailed()) {
            throw HyracksDataException.create(callback.getFailure());
        }
        // Force modified metadata page to disk.
        // If the index is not durable, then the flush is not necessary.
        if (forceToDisk) {
            bufferCache.force(fileId, true);
        }
    }

    public static void markAsValid(IBufferCache bufferCache, BloomFilter filter, boolean forceToDisk)
            throws HyracksDataException {
        if (forceToDisk) {
            bufferCache.force(filter.getFileId(), true);
        }
    }
}
