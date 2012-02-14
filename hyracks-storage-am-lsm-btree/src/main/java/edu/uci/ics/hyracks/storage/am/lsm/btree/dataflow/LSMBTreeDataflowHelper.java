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

package edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;

public class LSMBTreeDataflowHelper extends TreeIndexDataflowHelper {
    private static int DEFAULT_MEM_NUM_PAGES = 1000;
    private static int DEFAULT_MEM_PAGE_SIZE = 32768;
    
    private final int memNumPages;
    private final int memPageSize;
    
    public LSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            boolean createIfNotExists) {
        super(opDesc, ctx, partition, createIfNotExists);
        memNumPages = DEFAULT_MEM_NUM_PAGES;
        memPageSize = DEFAULT_MEM_PAGE_SIZE;
    }
    
    public LSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            boolean createIfNotExists, int memNumPages, int memPageSize) {
        super(opDesc, ctx, partition, createIfNotExists);
        this.memNumPages = memNumPages;
        this.memPageSize = memPageSize;
    }
    
    @Override
    public ITreeIndex createIndexInstance() throws HyracksDataException {
        // TODO: Implement this next.
        return null;
    }
}
