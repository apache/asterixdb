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
package org.apache.hyracks.storage.am.common.build;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests how {@link IndexBuilder} clears a leftover resource before creating an index.
 */
public class IndexBuilderTest {

    private static final String RESOURCE_PATH = "storage/partition_5/Default/Default/airline/0/airline";
    private static final long LEFTOVER_RESOURCE_ID = 693;
    private static final long NEW_RESOURCE_ID = 767;

    /**
     * A leftover resource that turns out to be already gone must not fail the create: the repository can report it
     * as present from a cached view of a storage partition whose resources were deleted by another node.
     */
    @Test
    public void createSucceedsWhenLeftoverResourceIsAlreadyGone() throws Exception {
        Fixture fixture = new Fixture();
        doThrow(HyracksDataException.create(ErrorCode.RESOURCE_DOES_NOT_EXIST, RESOURCE_PATH))
                .when(fixture.localResourceRepository).delete(RESOURCE_PATH);
        fixture.builder.build();
        verify(fixture.index).create();
        verify(fixture.localResourceRepository).insert(any());
        verify(fixture.lcManager).register(RESOURCE_PATH, fixture.index);
    }

    /**
     * Any other failure to clear the leftover resource must still fail the create.
     */
    @Test
    public void createFailsWhenLeftoverResourceCannotBeDeleted() throws Exception {
        Fixture fixture = new Fixture();
        doThrow(HyracksDataException.create(ErrorCode.CANNOT_DELETE_FILE, RESOURCE_PATH))
                .when(fixture.localResourceRepository).delete(RESOURCE_PATH);
        HyracksDataException failure = Assert.assertThrows(HyracksDataException.class, () -> fixture.builder.build());
        Assert.assertTrue(failure.matches(ErrorCode.CANNOT_DELETE_FILE));
        verify(fixture.index, never()).create();
        verify(fixture.localResourceRepository, never()).insert(any());
    }

    private static class Fixture {

        private final ILocalResourceRepository localResourceRepository = mock(ILocalResourceRepository.class);
        private final IResourceLifecycleManager<IIndex> lcManager = mock(IResourceLifecycleManager.class);
        private final IIndex index = mock(IIndex.class);
        private final IndexBuilder builder;

        @SuppressWarnings("unchecked")
        Fixture() throws HyracksDataException {
            INCServiceContext ctx = mock(INCServiceContext.class);
            IIOManager ioManager = mock(IIOManager.class);
            FileReference resourceRef = mock(FileReference.class);
            FileReference resolvedResourceRef = mock(FileReference.class);
            IStorageManager storageManager = mock(IStorageManager.class);
            IResourceIdFactory resourceIdFactory = mock(IResourceIdFactory.class);
            IResourceFactory localResourceFactory = mock(IResourceFactory.class);
            IResource resource = mock(IResource.class);
            LocalResource leftover = mock(LocalResource.class);

            when(ctx.getIoManager()).thenReturn(ioManager);
            when(resourceRef.getRelativePath()).thenReturn(RESOURCE_PATH);
            when(ioManager.resolve(RESOURCE_PATH)).thenReturn(resolvedResourceRef);
            // the index files are gone along with the resource
            when(resolvedResourceRef.getFile()).thenReturn(new File(RESOURCE_PATH));
            when(storageManager.getLifecycleManager(ctx)).thenReturn(lcManager);
            when(storageManager.getLocalResourceRepository(ctx)).thenReturn(localResourceRepository);
            // the repository reports a leftover resource at the path the index is about to be created at
            when(leftover.getId()).thenReturn(LEFTOVER_RESOURCE_ID);
            when(localResourceRepository.get(RESOURCE_PATH)).thenReturn(leftover);
            when(resourceIdFactory.createId()).thenReturn(NEW_RESOURCE_ID);
            when(localResourceFactory.createResource(resourceRef)).thenReturn(resource);
            when(resource.createInstance(ctx)).thenReturn(index);
            when(lcManager.get(anyString())).thenReturn(null);

            builder = new IndexBuilder(ctx, storageManager, resourceIdFactory, resourceRef, localResourceFactory, true);
        }
    }
}
