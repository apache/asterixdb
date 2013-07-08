/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.nc.io;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.api.resources.IDeallocatableRegistry;

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