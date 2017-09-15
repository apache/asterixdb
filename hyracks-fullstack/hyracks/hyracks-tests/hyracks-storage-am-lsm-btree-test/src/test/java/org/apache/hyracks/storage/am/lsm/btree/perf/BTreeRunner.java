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

package org.apache.hyracks.storage.am.lsm.btree.perf;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrameFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;

public class BTreeRunner extends InMemoryBTreeRunner {
    protected static final int MAX_OPEN_FILES = 10;
    protected static final int HYRACKS_FRAME_SIZE = 128;

    public BTreeRunner(int numTuples, int pageSize, int numPages, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories) throws HyracksDataException {
        super(numTuples, pageSize, numPages, typeTraits, cmpFactories);
    }

    @Override
    protected void init(int pageSize, int numPages, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories)
            throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);
        TestStorageManagerComponentHolder.init(pageSize, numPages, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        ITreeIndexMetadataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        LinkedMetaDataPageManager freePageManager = new LinkedMetaDataPageManager(bufferCache, metaFrameFactory);
        btree = BTreeUtils.createBTree(bufferCache, typeTraits, cmpFactories, BTreeLeafFrameType.REGULAR_NSM, file,
                freePageManager, false);
    }
}
