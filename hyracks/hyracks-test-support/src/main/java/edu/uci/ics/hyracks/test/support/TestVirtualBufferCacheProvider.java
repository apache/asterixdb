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
package edu.uci.ics.hyracks.test.support;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;

public class TestVirtualBufferCacheProvider implements IVirtualBufferCacheProvider {

    private static final long serialVersionUID = 1L;

    private final int pageSize;
    private final int numPages;

    public TestVirtualBufferCacheProvider(int pageSize, int numPages) {
        this.pageSize = pageSize;
        this.numPages = numPages;
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(IHyracksTaskContext ctx) {
        List<IVirtualBufferCache> vbcs = new ArrayList<IVirtualBufferCache>();
        for (int i = 0; i < 2; i++) {
            IVirtualBufferCache vbc = new VirtualBufferCache(new HeapBufferAllocator(), pageSize, numPages / 2);
            vbcs.add(vbc);
        }
        return vbcs;
    }
}
