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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.api.resources.IDeallocatableRegistry;

public final class WorkspaceFileFactory implements IWorkspaceFileFactory {
    private final IDeallocatableRegistry registry;
    private final IOManager ioManager;

    public WorkspaceFileFactory(IDeallocatableRegistry registry, IOManager ioManager) {
        this.registry = registry;
        this.ioManager = ioManager;
    }

    @Override
    public FileReference createManagedWorkspaceFile(String prefix) throws HyracksDataException {
        final FileReference fRef = ioManager.createWorkspaceFile(prefix);
        registry.registerDeallocatable(new IDeallocatable() {
            @Override
            public void deallocate() {
                fRef.delete();
            }
        });
        return fRef;
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException {
        return ioManager.createWorkspaceFile(prefix);
    }
}