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
package org.apache.asterix.cloud.lazy.accessor;

import static org.apache.asterix.cloud.util.CloudFileUtil.DATA_FILTER;

import java.util.Collection;
import java.util.Set;

import org.apache.asterix.cloud.UncachedFileReference;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.lazy.IParallelCacher;
import org.apache.asterix.cloud.lazy.filesystem.IHolePuncher;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.control.nc.io.IOManager;

public class SelectiveCloudAccessor extends ReplaceableCloudAccessor {
    private final IHolePuncher puncher;

    public SelectiveCloudAccessor(ICloudClient cloudClient, String bucket, IOManager localIoManager,
            Set<Integer> partitions, IHolePuncher puncher, IParallelCacher cacher) {
        super(cloudClient, bucket, localIoManager, partitions, InitialCloudAccessor.NO_OP_REPLACER, cacher);
        this.puncher = puncher;
    }

    @Override
    public int doPunchHole(IFileHandle fileHandle, long offset, long length) throws HyracksDataException {
        try {
            // Ensure that punching a hole cannot be interrupted
            InvokeUtil.doExUninterruptibly(() -> puncher.punchHole(fileHandle, offset, length));
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }

        return (int) length;
    }

    @Override
    public void doEvict(FileReference directory) throws HyracksDataException {
        if (!localIoManager.exists(directory)) {
            return;
        }

        if (!directory.getFile().isDirectory()) {
            throw new IllegalStateException(directory + " is not a directory");
        }

        // Get a list of all data files
        Collection<FileReference> files = localIoManager.list(directory, DATA_FILTER);
        if (files.isEmpty()) {
            // Nothing to evict
            return;
        }

        // Convert file references to uncached ones
        Collection<FileReference> uncachedFiles = UncachedFileReference.toUncached(files);
        // Add all data files to the cacher to indicate they are in a 'cacheable' state (i.e., not downloaded)
        cacher.add(uncachedFiles);
        // Delete all data files from the local drive
        for (FileReference uncachedFile : uncachedFiles) {
            localIoManager.delete(uncachedFile);
        }
    }

    @Override
    protected void replace() {
        // NoOp
    }
}
