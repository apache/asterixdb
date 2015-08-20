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

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public class TestStorageManagerInterface implements IStorageManagerInterface {
    private static final long serialVersionUID = 1L;

    @Override
    public IBufferCache getBufferCache(IHyracksTaskContext ctx) {
        return TestStorageManagerComponentHolder.getBufferCache(ctx);
    }

    @Override
    public IFileMapProvider getFileMapProvider(IHyracksTaskContext ctx) {
        return TestStorageManagerComponentHolder.getFileMapProvider(ctx);
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository(IHyracksTaskContext ctx) {
        return TestStorageManagerComponentHolder.getLocalResourceRepository(ctx);
    }

	@Override
	public ResourceIdFactory getResourceIdFactory(IHyracksTaskContext ctx) {
		return TestStorageManagerComponentHolder.getResourceIdFactory(ctx);
	}
}