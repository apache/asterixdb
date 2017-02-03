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
package org.apache.hyracks.storage.am.lsm.common.utils;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;

public class ComponentMetadataUtil {

    public static final MutableArrayValueReference MARKER_LSN_KEY =
            new MutableArrayValueReference("Marker".getBytes());
    public static final long NOT_FOUND = -1L;

    private ComponentMetadataUtil() {
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
    public static long getLong(IComponentMetadata metadata, IValueReference key, long defaultValue)
            throws HyracksDataException {
        IValueReference value = metadata.get(key);
        return value == null || value.getLength() == 0 ? defaultValue
                : LongPointable.getLong(value.getByteArray(), value.getStartOffset());
    }

    /**
     * Get a value from an index's metadata pages. It first, searches the current in memory component
     * then searches the other components. in reverse order.
     * Note: This method locks on the OpTracker of the index
     *
     * @param index
     * @param key
     * @param pointable
     * @throws HyracksDataException
     */
    public static void get(ILSMIndex index, IValueReference key, IPointable pointable) throws HyracksDataException {
        // Lock the opTracker to ensure index components don't change
        synchronized (index.getOperationTracker()) {
            index.getCurrentMemoryComponent().getMetadata().get(key, pointable);
            if (pointable.getLength() == 0) {
                // was not found in the in current mutable component, search in the other in memory components
                fromImmutableMemoryComponents(index, key, pointable);
                if (pointable.getLength() == 0) {
                    // was not found in the in all in memory components, search in the disk components
                    fromDiskComponents(index, key, pointable);
                }
            }
        }
    }

    private static void fromDiskComponents(ILSMIndex index, IValueReference key, IPointable pointable)
            throws HyracksDataException {
        for (ILSMDiskComponent c : index.getImmutableComponents()) {
            c.getMetadata().get(key, pointable);
            if (pointable.getLength() != 0) {
                // Found
                return;
            }
        }
    }

    private static void fromImmutableMemoryComponents(ILSMIndex index, IValueReference key, IPointable pointable) {
        List<ILSMMemoryComponent> memComponents = index.getMemoryComponents();
        int numOtherMemComponents = memComponents.size() - 1;
        int next = index.getCurrentMemoryComponentIndex();
        for (int i = 0; i < numOtherMemComponents; i++) {
            next = next - 1;
            if (next < 0) {
                next = memComponents.size() - 1;
            }
            ILSMMemoryComponent c = index.getMemoryComponents().get(next);
            if (c.isReadable()) {
                c.getMetadata().get(key, pointable);
                if (pointable.getLength() != 0) {
                    // Found
                    return;
                }
            }
        }
    }
}
