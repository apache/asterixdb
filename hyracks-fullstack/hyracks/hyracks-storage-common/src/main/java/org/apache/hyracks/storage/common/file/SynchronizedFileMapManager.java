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
package org.apache.hyracks.storage.common.file;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

public class SynchronizedFileMapManager implements IFileMapManager {
    private final IFileMapManager delegate;

    public SynchronizedFileMapManager(IFileMapManager fileMapManager) {
        this.delegate = fileMapManager;
    }

    @Override
    public int registerFile(FileReference fileRef) throws HyracksDataException {
        synchronized (delegate) {
            return delegate.registerFile(fileRef);
        }
    }

    @Override
    public int registerFileIfAbsent(FileReference fileRef) {
        synchronized (delegate) {
            return delegate.registerFileIfAbsent(fileRef);
        }
    }

    @Override
    public FileReference unregisterFile(int fileId) throws HyracksDataException {
        synchronized (delegate) {
            return delegate.unregisterFile(fileId);
        }
    }

    @Override
    public int unregisterFile(FileReference fileRef) throws HyracksDataException {
        synchronized (delegate) {
            return delegate.unregisterFile(fileRef);
        }
    }

    @Override
    public boolean isMapped(int fileId) {
        synchronized (delegate) {
            return delegate.isMapped(fileId);
        }
    }

    @Override
    public boolean isMapped(FileReference fileRef) {
        synchronized (delegate) {
            return delegate.isMapped(fileRef);
        }
    }

    @Override
    public int lookupFileId(FileReference fileRef) throws HyracksDataException {
        synchronized (delegate) {
            return delegate.lookupFileId(fileRef);
        }
    }

    @Override
    public FileReference lookupFileName(int fileId) throws HyracksDataException {
        synchronized (delegate) {
            return delegate.lookupFileName(fileId);
        }
    }
}
