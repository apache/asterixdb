/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.util.IndexUtils;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;

public final class InvertedIndexDataflowHelper extends IndexDataflowHelper {
    private final TreeIndexDataflowHelper btreeDataflowHelper;

    public InvertedIndexDataflowHelper(TreeIndexDataflowHelper btreeDataflowHelper, IIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, boolean createIfNotExists) {
        super(opDesc, ctx, partition, createIfNotExists);
        this.btreeDataflowHelper = btreeDataflowHelper;
    }

    public FileReference getFilereference() {
        AbstractInvertedIndexOperatorDescriptor invIndexOpDesc = (AbstractInvertedIndexOperatorDescriptor) opDesc;
        IFileSplitProvider fileSplitProvider = invIndexOpDesc.getInvListsFileSplitProvider();
        return fileSplitProvider.getFileSplits()[partition].getLocalFile();
    }

    @Override
    public IIndex createIndexInstance() throws HyracksDataException {
        IInvertedIndexOperatorDescriptor invIndexOpDesc = (IInvertedIndexOperatorDescriptor) opDesc;
        MultiComparator cmp = IndexUtils.createMultiComparator(invIndexOpDesc.getInvListsComparatorFactories());
        // Assumes btreeDataflowHelper.init() has already been called.
        BTree btree = (BTree) btreeDataflowHelper.getIndex();
        return new InvertedIndex(opDesc.getStorageManager().getBufferCache(ctx), btree,
                invIndexOpDesc.getInvListsTypeTraits(), cmp);
    }
}