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

package edu.uci.ics.hyracks.storage.am.lsm.btree.perf;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class BTreeRunner extends InMemoryBTreeRunner {
    protected static final int MAX_OPEN_FILES = 10;
    protected static final int HYRACKS_FRAME_SIZE = 128;

    public BTreeRunner(int numTuples, int pageSize, int numPages, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories) throws HyracksDataException, BTreeException {
        super(numTuples, pageSize, numPages, typeTraits, cmpFactories);
    }

    @Override
    protected void init(int pageSize, int numPages, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories)
            throws HyracksDataException, BTreeException {
        IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);
        TestStorageManagerComponentHolder.init(pageSize, numPages, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        btree = BTreeUtils
                .createBTree(bufferCache, fmp, typeTraits, cmpFactories, BTreeLeafFrameType.REGULAR_NSM, file);
    }
}
