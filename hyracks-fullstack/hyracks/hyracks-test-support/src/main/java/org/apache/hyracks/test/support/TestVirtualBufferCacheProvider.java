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
package org.apache.hyracks.test.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;

public class TestVirtualBufferCacheProvider implements IVirtualBufferCacheProvider {

    private static final long serialVersionUID = 1L;

    private final int pageSize;
    private final int numPages;

    public TestVirtualBufferCacheProvider(int pageSize, int numPages) {
        this.pageSize = pageSize;
        this.numPages = numPages;
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(INCServiceContext ctx, FileReference fileRef)
            throws HyracksDataException {
        List<IVirtualBufferCache> vbcs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            IVirtualBufferCache vbc = new VirtualBufferCache(new HeapBufferAllocator(), pageSize, numPages / 2);
            vbcs.add(vbc);
        }
        return vbcs;
    }
}
