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
package org.apache.hyracks.cloud.test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.cloud.io.ICloudProperties;
import org.apache.hyracks.cloud.io.stream.CloudInputStream;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;
import org.apache.hyracks.control.nc.io.IOManager;

/**
 * Serves cloud reads from an in-memory copy of each file, recording each request as {offset, length}, records local
 * writes the same way, and punches holes by writing zeros.
 */
public final class FakeCloudIOManager extends IOManager implements ICloudIOManager {
    /**
     * The cloud copy of each file, by its relative path
     */
    public final Map<String, byte[]> remote = new HashMap<>();
    public final List<long[]> cloudRequests = new ArrayList<>();
    public final List<long[]> localWrites = new ArrayList<>();
    public final List<long[]> punches = new ArrayList<>();

    public FakeCloudIOManager(List<IODeviceHandle> devices) throws HyracksDataException {
        super(devices, new DefaultDeviceResolver(), 1, 10);
    }

    @Override
    public void cloudRead(IFileHandle fHandle, long offset, ByteBuffer data) {
        cloudRequests.add(new long[] { offset, data.remaining() });
        data.put(remote.get(fHandle.getFileReference().getRelativePath()), Math.toIntExact(offset), data.remaining());
    }

    @Override
    public CloudInputStream cloudRead(IFileHandle fHandle, long offset, long length) {
        cloudRequests.add(new long[] { offset, length });
        byte[] bytes = remote.get(fHandle.getFileReference().getRelativePath());
        InputStream in = new ByteArrayInputStream(bytes, Math.toIntExact(offset), Math.toIntExact(length));
        return new CloudInputStream(this, fHandle, in, offset, length);
    }

    @Override
    public int localWriter(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        localWrites.add(new long[] { offset, data.remaining() });
        return doSyncWrite(fHandle, offset, data);
    }

    @Override
    public int punchHole(IFileHandle fHandle, long offset, long length) throws HyracksDataException {
        punches.add(new long[] { offset, length });
        doSyncWrite(fHandle, offset, ByteBuffer.allocate(Math.toIntExact(length)));
        return (int) length;
    }

    @Override
    public IIOManager getLocalIOManager() {
        return this;
    }

    @Override
    public void downloadLibrary(Collection<FileReference> libPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreStream(CloudInputStream stream) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long localWriter(IFileHandle fHandle, long offset, ByteBuffer[] data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int cloudWrite(IFileHandle fHandle, long offset, ByteBuffer data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cloudWrite(IFileHandle fHandle, long offset, ByteBuffer[] data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void evict(String resourcePath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reloadConfiguration(ICloudProperties newProperties) {
        throw new UnsupportedOperationException();
    }
}
