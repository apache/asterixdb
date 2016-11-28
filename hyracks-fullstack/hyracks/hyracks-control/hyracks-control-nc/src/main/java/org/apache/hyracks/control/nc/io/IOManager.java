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
package org.apache.hyracks.control.nc.io;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileDeviceComputer;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOFuture;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;

public class IOManager implements IIOManager {
    /*
     * Constants
     */
    private static final String WORKSPACE_FILE_SUFFIX = ".waf";
    private static final FilenameFilter WORKSPACE_FILES_FILTER = (dir, name) -> name.endsWith(WORKSPACE_FILE_SUFFIX);
    /*
     * Finals
     */
    private final List<IODeviceHandle> ioDevices;
    private final List<IODeviceHandle> workspaces;
    /*
     * Mutables
     */
    private Executor executor;
    private int workspaceIndex;
    private IFileDeviceComputer deviceComputer;

    public IOManager(List<IODeviceHandle> devices, Executor executor) throws HyracksDataException {
        this(devices);
        this.executor = executor;
    }

    public IOManager(List<IODeviceHandle> devices) throws HyracksDataException {
        this.ioDevices = Collections.unmodifiableList(devices);
        workspaces = new ArrayList<>();
        for (IODeviceHandle d : ioDevices) {
            if (d.getWorkspace() != null) {
                new File(d.getMount(), d.getWorkspace()).mkdirs();
                workspaces.add(d);
            }
        }
        if (workspaces.isEmpty()) {
            throw new HyracksDataException("No devices with workspace found");
        }
        workspaceIndex = 0;
        deviceComputer = new DefaultDeviceComputer(this);
    }

    @Override
    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public List<IODeviceHandle> getIODevices() {
        return ioDevices;
    }

    @Override
    public IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException {
        FileHandle fHandle = new FileHandle(fileRef);
        try {
            fHandle.open(rwMode, syncMode);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return fHandle;
    }

    @Override
    public int syncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        try {
            if (fHandle == null) {
                throw new IllegalStateException("Trying to write to a deleted file.");
            }
            int n = 0;
            int remaining = data.remaining();
            while (remaining > 0) {
                int len = ((FileHandle) fHandle).getFileChannel().write(data, offset);
                if (len < 0) {
                    throw new HyracksDataException(
                            "Error writing to file: " + ((FileHandle) fHandle).getFileReference().toString());
                }
                remaining -= len;
                offset += len;
                n += len;
            }
            return n;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public long syncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException {
        try {
            if (fHandle == null) {
                throw new IllegalStateException("Trying to write to a deleted file.");
            }
            int n = 0;
            int remaining = 0;
            for (ByteBuffer buf : dataArray) {
                remaining += buf.remaining();
            }
            final FileChannel fileChannel = ((FileHandle) fHandle).getFileChannel();
            while (remaining > 0) {
                long len;
                synchronized (fileChannel) {
                    fileChannel.position(offset);
                    len = fileChannel.write(dataArray);
                }
                if (len < 0) {
                    throw new HyracksDataException(
                            "Error writing to file: " + ((FileHandle) fHandle).getFileReference().toString());
                }
                remaining -= len;
                offset += len;
                n += len;
            }
            return n;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * Please do check the return value of this read!
     *
     * @param fHandle
     * @param offset
     * @param data
     * @return The number of bytes read, possibly zero, or -1 if the given offset is greater than or equal to the file's
     *         current size
     * @throws HyracksDataException
     */
    @Override
    public int syncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        try {
            int n = 0;
            int remaining = data.remaining();
            while (remaining > 0) {
                int len = ((FileHandle) fHandle).getFileChannel().read(data, offset);
                if (len < 0) {
                    return n == 0 ? -1 : n;
                }
                remaining -= len;
                offset += len;
                n += len;
            }
            return n;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public IIOFuture asyncWrite(IFileHandle fHandle, long offset, ByteBuffer data) {
        AsyncWriteRequest req = new AsyncWriteRequest((FileHandle) fHandle, offset, data);
        executor.execute(req);
        return req;
    }

    @Override
    public IIOFuture asyncRead(IFileHandle fHandle, long offset, ByteBuffer data) {
        AsyncReadRequest req = new AsyncReadRequest((FileHandle) fHandle, offset, data);
        executor.execute(req);
        return req;
    }

    @Override
    public void close(IFileHandle fHandle) throws HyracksDataException {
        try {
            ((FileHandle) fHandle).close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public synchronized FileReference createWorkspaceFile(String prefix) throws HyracksDataException {
        IODeviceHandle dev = workspaces.get(workspaceIndex);
        workspaceIndex = (workspaceIndex + 1) % workspaces.size();
        String waPath = dev.getWorkspace();
        File waf;
        try {
            waf = File.createTempFile(prefix, WORKSPACE_FILE_SUFFIX, new File(dev.getMount(), waPath));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return dev.createFileRef(waPath + File.separator + waf.getName());
    }

    private abstract class AsyncRequest implements IIOFuture, Runnable {
        protected final FileHandle fHandle;
        protected final long offset;
        protected final ByteBuffer data;
        private boolean complete;
        private HyracksDataException exception;
        private int result;

        private AsyncRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            this.fHandle = fHandle;
            this.offset = offset;
            this.data = data;
            complete = false;
            exception = null;
        }

        @Override
        public void run() {
            HyracksDataException hde = null;
            int res = -1;
            try {
                res = performOperation();
            } catch (HyracksDataException e) {
                hde = e;
            }
            synchronized (this) {
                exception = hde;
                result = res;
                complete = true;
                notifyAll();
            }
        }

        protected abstract int performOperation() throws HyracksDataException;

        @Override
        public synchronized int synchronize() throws HyracksDataException, InterruptedException {
            while (!complete) {
                wait();
            }
            if (exception != null) {
                throw exception;
            }
            return result;
        }

        @Override
        public synchronized boolean isComplete() {
            return complete;
        }
    }

    private class AsyncReadRequest extends AsyncRequest {
        private AsyncReadRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            super(fHandle, offset, data);
        }

        @Override
        protected int performOperation() throws HyracksDataException {
            return syncRead(fHandle, offset, data);
        }
    }

    private class AsyncWriteRequest extends AsyncRequest {
        private AsyncWriteRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            super(fHandle, offset, data);
        }

        @Override
        protected int performOperation() throws HyracksDataException {
            return syncWrite(fHandle, offset, data);
        }
    }

    @Override
    public void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException {
        try {
            ((FileHandle) fileHandle).sync(metadata);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public long getSize(IFileHandle fileHandle) {
        return ((FileHandle) fileHandle).getFileReference().getFile().length();
    }

    @Override
    public void deleteWorkspaceFiles() {
        for (IODeviceHandle ioDevice : workspaces) {
            File workspaceFolder = new File(ioDevice.getMount(), ioDevice.getWorkspace());
            if (workspaceFolder.exists() && workspaceFolder.isDirectory()) {
                File[] workspaceFiles = workspaceFolder.listFiles(WORKSPACE_FILES_FILTER);
                for (File workspaceFile : workspaceFiles) {
                    workspaceFile.delete();
                }
            }
        }
    }

    @Override
    public synchronized FileReference getFileReference(int ioDeviceId, String relativePath) {
        IODeviceHandle devHandle = ioDevices.get(ioDeviceId);
        return new FileReference(devHandle, relativePath);
    }

    @Override
    public FileReference resolve(String path) throws HyracksDataException {
        return new FileReference(deviceComputer.compute(path), path);
    }

    @Override
    public FileReference resolveAbsolutePath(String path) throws HyracksDataException {
        IODeviceHandle devHandle = getDevice(path);
        if (devHandle == null) {
            throw new HyracksDataException("The file with absolute path: " + path + " is outside all io devices");
        }
        String relativePath = devHandle.getRelativePath(path);
        return new FileReference(devHandle, relativePath);
    }

    public IODeviceHandle getDevice(String fullPath) {
        for (IODeviceHandle d : ioDevices) {
            if (fullPath.startsWith(d.getMount().getAbsolutePath())) {
                return d;
            }
        }
        return null;
    }
}
