/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.IndexFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class InvertedIndexFactory extends IndexFactory<IIndex> {

    protected ITypeTraits[] invListTypeTraits;
    protected IBinaryComparatorFactory[] invListCmpFactories;
    protected IInvertedListBuilder invListBuilder;
    protected IBinaryTokenizer tokenizer;
    protected int numTokenFields;
    protected int numInvListKeys;
    
    protected RangePredicate btreePred;
    protected ITreeIndexFrame leafFrame;
    protected ITreeIndexCursor btreeCursor;
    protected MultiComparator searchCmp;
    
    protected ILSMFileManager fileManager;
    
    public InvertedIndexFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IFreePageManagerFactory freePageManagerFactory, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, IInvertedListBuilder invListBuilder,
            IBinaryTokenizer tokenizer, ILSMFileManager fileManager) {
        super(bufferCache, fileMapProvider, freePageManagerFactory);
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.invListBuilder = invListBuilder;
        this.tokenizer = tokenizer;
        //this.numTokenFields = btree.getComparatorFactories().length;
        this.numInvListKeys = invListCmpFactories.length;

        // setup for cursor creation
//        
//        btreePred = new RangePredicate(null, null, true, true, null, null);
//        leafFrame = btree.getLeafFrameFactory().createFrame();
//        btreeCursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) leafFrame, false);
//        searchCmp = MultiComparator.create(btree.getComparatorFactories());
//        btreePred.setLowKeyComparator(searchCmp);
//        btreePred.setHighKeyComparator(searchCmp);
        
        // fileManager for creating a file of a diskInvertedIndex
        this.fileManager = fileManager;
    }

    @Override
    public IIndex createIndexInstance(FileReference file) {
        return new InvertedIndex(bufferCache, btree, invListTypeTraits, invListCmpFactories, invListBuilder,
                tokenizer); ;
    }
}
