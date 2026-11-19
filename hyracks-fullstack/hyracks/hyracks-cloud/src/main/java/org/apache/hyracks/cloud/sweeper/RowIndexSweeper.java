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
package org.apache.hyracks.cloud.sweeper;

import static org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState.READABLE_UNWRITABLE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.cache.unit.UnsweepableIndexUnit;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Punches the local copies of a row-based index's disk components back to holes, a whole file at a time, so that
 * their pages are read from the cloud storage the next time they are needed.
 * <p>
 * Only files that allow holes are punched, which bloom filters and look-aside files never do: they are downloaded
 * whole and are not read from the cloud storage page by page. A file is punched only while no page of it is being
 * loaded, and loads that arrive meanwhile wait for the punch (see {@link BufferedFileHandle#tryPunch}). The pages in
 * the buffer cache are punched too: a page that is not on disk is read again from the cloud storage only once it
 * leaves the buffer cache.
 * <p>
 * Components are not entered while their files are punched: the files of a component that is merged away, or of an
 * index that is dropped, are closed before they are deleted, and a closed file is never punched.
 */
public final class RowIndexSweeper {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ICloudIOManager cloudIOManager;
    private final Map<Integer, BufferedFileHandle> fileInfoMap;

    public RowIndexSweeper(ICloudIOManager cloudIOManager, Map<Integer, BufferedFileHandle> fileInfoMap) {
        this.cloudIOManager = cloudIOManager;
        this.fileInfoMap = fileInfoMap;
    }

    /**
     * Punches the index's disk components, one file at a time, until {@code stop} says so
     *
     * @param indexUnit the index
     * @param stop      checked before each file
     */
    public void sweep(UnsweepableIndexUnit indexUnit, BooleanSupplier stop) throws HyracksDataException {
        List<ILSMDiskComponent> components = getSweepableComponents(indexUnit.getIndex());
        int punched = 0;
        for (int i = 0; i < components.size() && !stop.getAsBoolean(); i++) {
            BufferedFileHandle file = getPunchableFile(components.get(i));
            if (file != null && sweepFile(file)) {
                punched++;
            }
        }
        if (punched > 0) {
            LOGGER.info("Punched {} files of {}", punched, indexUnit);
        }
    }

    private static List<ILSMDiskComponent> getSweepableComponents(ILSMIndex index) {
        List<ILSMDiskComponent> components = new ArrayList<>();
        synchronized (index.getOperationTracker()) {
            for (ILSMDiskComponent component : index.getDiskComponents()) {
                // a component being merged will be deleted, and one being written is not complete
                if (component.getState() == READABLE_UNWRITABLE && component.getComponentSize() > 0) {
                    components.add(component);
                }
            }
        }
        return components;
    }

    /**
     * @return the component's tree file, the only one of its files that can allow holes, or null if it does not or
     *         is not open
     */
    private BufferedFileHandle getPunchableFile(ILSMDiskComponent component) {
        if (!(component.getIndex() instanceof ITreeIndex tree)) {
            return null;
        }
        synchronized (fileInfoMap) {
            BufferedFileHandle handle = fileInfoMap.get(tree.getFileId());
            if (handle != null && handle.hasBeenOpened() && !handle.hasBeenDeleted()
                    && handle.getFileReference().areHolesAllowed()) {
                return handle;
            }
        }
        return null;
    }

    /**
     * Punches the whole of the file, unless a page of it is being loaded or it has been closed
     *
     * @param handle a file of a finished disk component
     * @return whether the file was punched
     */
    boolean sweepFile(BufferedFileHandle handle) throws HyracksDataException {
        return handle.tryPunch(fh -> {
            long fileSize = handle.getFileSize();
            // fallocate rejects an empty range
            return fileSize > 0 ? cloudIOManager.punchHole(fh, 0L, fileSize) : 0;
        });
    }
}
