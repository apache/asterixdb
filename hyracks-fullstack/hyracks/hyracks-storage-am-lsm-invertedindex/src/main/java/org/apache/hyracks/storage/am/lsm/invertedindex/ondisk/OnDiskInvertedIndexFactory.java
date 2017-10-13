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
package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexFileNameMapper;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilderFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class OnDiskInvertedIndexFactory extends IndexFactory<IInvertedIndex> {

    protected final IInvertedListBuilderFactory invListBuilderFactory;
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    protected final IInvertedIndexFileNameMapper fileNameMapper;

    public OnDiskInvertedIndexFactory(IIOManager ioManager, IBufferCache bufferCache,
            IInvertedListBuilderFactory invListBuilderFactory, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IInvertedIndexFileNameMapper fileNameMapper,
            IPageManagerFactory pageManagerFactory) {
        super(ioManager, bufferCache, pageManagerFactory);
        this.invListBuilderFactory = invListBuilderFactory;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.fileNameMapper = fileNameMapper;
    }

    @Override
    public OnDiskInvertedIndex createIndexInstance(FileReference dictBTreeFile) throws HyracksDataException {
        String invListsFilePath = fileNameMapper.getInvListsFilePath(dictBTreeFile.getFile().getAbsolutePath());
        FileReference invListsFile = ioManager.resolveAbsolutePath(invListsFilePath);
        IInvertedListBuilder invListBuilder = invListBuilderFactory.create();
        return new OnDiskInvertedIndex(bufferCache, invListBuilder, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, dictBTreeFile, invListsFile, freePageManagerFactory);
    }
}
