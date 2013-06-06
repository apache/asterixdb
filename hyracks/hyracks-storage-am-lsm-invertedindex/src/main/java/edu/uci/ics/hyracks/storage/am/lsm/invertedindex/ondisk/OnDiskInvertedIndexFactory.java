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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.io.File;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.IndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexFileNameMapper;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilderFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class OnDiskInvertedIndexFactory extends IndexFactory<IInvertedIndex> {

    protected final IInvertedListBuilderFactory invListBuilderFactory;
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    protected final IInvertedIndexFileNameMapper fileNameMapper;

    public OnDiskInvertedIndexFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IInvertedListBuilderFactory invListBuilderFactory, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IInvertedIndexFileNameMapper fileNameMapper) {
        super(bufferCache, fileMapProvider, null);
        this.invListBuilderFactory = invListBuilderFactory;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.fileNameMapper = fileNameMapper;
    }

    @Override
    public IInvertedIndex createIndexInstance(FileReference dictBTreeFile) throws IndexException {
        String invListsFilePath = fileNameMapper.getInvListsFilePath(dictBTreeFile.getFile().getPath());
        FileReference invListsFile = new FileReference(new File(invListsFilePath));
        IInvertedListBuilder invListBuilder = invListBuilderFactory.create();
        return new OnDiskInvertedIndex(bufferCache, fileMapProvider, invListBuilder, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, dictBTreeFile, invListsFile);
    }
}
