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

import java.util.Collection;
import java.util.Set;

import org.apache.asterix.cloud.UncachedFileReference;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.lazy.IParallelCacher;
import org.apache.asterix.cloud.lazy.filesystem.IHolePuncher;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
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
        return puncher.punchHole(fileHandle, offset, length);
    }

    @Override
    public void doEvict(FileReference directory) throws HyracksDataException {
        if (!directory.getFile().isDirectory()) {
            throw new IllegalStateException(directory + " is not a directory");
        }

        // TODO only delete data files?
        Collection<FileReference> uncachedFiles = UncachedFileReference.toUncached(localIoManager.list(directory));
        cacher.add(uncachedFiles);
        localIoManager.delete(directory);
    }

    @Override
    protected void replace() {
        // NoOp
    }
}
