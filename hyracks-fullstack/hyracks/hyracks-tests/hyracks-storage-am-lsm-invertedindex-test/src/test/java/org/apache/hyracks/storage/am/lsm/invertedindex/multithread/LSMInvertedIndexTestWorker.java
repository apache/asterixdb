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

package org.apache.hyracks.storage.am.lsm.invertedindex.multithread;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.AbstractIndexTestWorker;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.TestOperationSelector;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.JaccardSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils.HyracksTaskTestContext;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;

public class LSMInvertedIndexTestWorker extends AbstractIndexTestWorker {

    protected final LSMInvertedIndex invIndex;
    protected final List<ITupleReference> documentCorpus = new ArrayList<>();
    protected final Random rnd = new Random(50);

    protected final IInvertedIndexSearchModifier[] TEST_SEARCH_MODIFIERS = new IInvertedIndexSearchModifier[] {
            new ConjunctiveSearchModifier(), new JaccardSearchModifier(0.8f), new JaccardSearchModifier(0.5f) };

    public LSMInvertedIndexTestWorker(DataGenThread dataGen, TestOperationSelector opSelector, IIndex index,
            int numBatches) throws HyracksDataException {
        super(dataGen, opSelector, index, numBatches);
        // Dummy hyracks task context for the test purpose only
        IHyracksTaskContext ctx = new HyracksTaskTestContext();
        // Intermediate and final search result will use this buffer manager to get frames.
        IDeallocatableFramePool framePool = new DeallocatableFramePool(ctx,
                AccessMethodTestsConfig.LSM_INVINDEX_SEARCH_FRAME_LIMIT * ctx.getInitialFrameSize());
        ISimpleFrameBufferManager bufferManagerForSearch = new FramePoolBackedFrameBufferManager(framePool);
        // Keep the buffer manager in the hyracks context so that the search process can get it via the context.
        TaskUtil.put(HyracksConstants.INVERTED_INDEX_SEARCH_FRAME_MANAGER, bufferManagerForSearch, ctx);
        IIndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(HyracksConstants.HYRACKS_TASK_CONTEXT, ctx);
        indexAccessor = index.createAccessor(iap);
        invIndex = (LSMInvertedIndex) index;
    }

    @Override
    public void performOp(ITupleReference tuple, TestOperation op) throws HyracksDataException {
        LSMInvertedIndexAccessor accessor = (LSMInvertedIndexAccessor) indexAccessor;
        IIndexCursor searchCursor = accessor.createSearchCursor(false);
        IIndexCursor rangeSearchCursor = accessor.createRangeSearchCursor();
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        IBinaryTokenizerFactory tokenizerFactory = invIndex.getTokenizerFactory();
        int searchModifierIndex = Math.abs(rnd.nextInt()) % TEST_SEARCH_MODIFIERS.length;
        InvertedIndexSearchPredicate searchPred = new InvertedIndexSearchPredicate(tokenizerFactory.createTokenizer(),
                TEST_SEARCH_MODIFIERS[searchModifierIndex]);

        switch (op) {
            case INSERT: {
                insert(accessor, tuple);
                break;
            }

            case DELETE: {
                // Randomly pick a document from the corpus to delete.
                if (!documentCorpus.isEmpty()) {
                    int docIndex = Math.abs(rnd.nextInt()) % documentCorpus.size();
                    ITupleReference deleteTuple = documentCorpus.get(docIndex);
                    accessor.delete(deleteTuple);
                    // Swap tupleIndex with last element.
                    documentCorpus.set(docIndex, documentCorpus.get(documentCorpus.size() - 1));
                    documentCorpus.remove(documentCorpus.size() - 1);
                } else {
                    // No existing documents to delete, treat this case as an insert.
                    insert(accessor, tuple);
                }
                break;
            }

            case POINT_SEARCH: {
                searchCursor.close();
                searchPred.setQueryTuple(tuple);
                searchPred.setQueryFieldIndex(0);
                try {
                    accessor.search(searchCursor, searchPred);
                    consumeCursorTuples(searchCursor);
                } catch (HyracksDataException e) {
                    // Ignore.
                    if (e.getErrorCode() != ErrorCode.OCCURRENCE_THRESHOLD_PANIC_EXCEPTION) {
                        throw e;
                    }
                }
                break;
            }

            case SCAN: {
                rangeSearchCursor.close();
                accessor.rangeSearch(rangeSearchCursor, rangePred);
                consumeCursorTuples(rangeSearchCursor);
                break;
            }

            case MERGE: {
                accessor.scheduleMerge(invIndex.getDiskComponents());
                break;
            }

            default:
                throw new HyracksDataException("Op " + op.toString() + " not supported.");
        }
    }

    private void insert(LSMInvertedIndexAccessor accessor, ITupleReference tuple) throws HyracksDataException {
        // Ignore ongoing merges. Do an insert instead.
        accessor.insert(tuple);
        // Add tuple to document corpus so we can delete it.
        ITupleReference copyTuple = TupleUtils.copyTuple(tuple);
        documentCorpus.add(copyTuple);
    }
}
