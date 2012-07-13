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
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;

public final class InvertedIndexDataflowHelper extends IndexDataflowHelper {

    public InvertedIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition) {
        super(opDesc, ctx, partition);
    }

    @Override
    public IIndex getIndexInstance() throws HyracksDataException {
        IInvertedIndexOperatorDescriptor invIndexOpDesc = (IInvertedIndexOperatorDescriptor) opDesc;
        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(
                invIndexOpDesc.getInvListsTypeTraits());
        return new InvertedIndex(opDesc.getStorageManager().getBufferCache(ctx), opDesc.getStorageManager()
                .getFileMapProvider(ctx), invListBuilder, invIndexOpDesc.getInvListsTypeTraits(),
                invIndexOpDesc.getInvListsComparatorFactories(), invIndexOpDesc.getTreeIndexTypeTraits(),
                invIndexOpDesc.getTreeIndexComparatorFactories(), file);
    }
}