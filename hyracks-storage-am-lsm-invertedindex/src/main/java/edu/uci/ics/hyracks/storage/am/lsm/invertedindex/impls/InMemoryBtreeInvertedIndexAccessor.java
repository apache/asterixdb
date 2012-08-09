package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexSearchCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexSearchPredicate;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.TOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;

public class InMemoryBtreeInvertedIndexAccessor implements IIndexAccessor {
    private final IHyracksCommonContext hyracksCtx = new DefaultHyracksCommonContext();
    private final IInvertedIndexSearcher searcher;
    protected IIndexOpContext ctx;
    protected InMemoryBtreeInvertedIndex memoryBtreeInvertedIndex;

    public InMemoryBtreeInvertedIndexAccessor(InMemoryBtreeInvertedIndex memoryBtreeInvertedIndex, IIndexOpContext ctx,
            IBinaryTokenizer tokenizer) {
        this.ctx = ctx;
        this.memoryBtreeInvertedIndex = memoryBtreeInvertedIndex;
        this.searcher = new TOccurrenceSearcher(hyracksCtx, memoryBtreeInvertedIndex, tokenizer);
    }

    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.INSERT);
        memoryBtreeInvertedIndex.insertUpdateOrDelete(tuple, ctx);
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub

    }

    @Override
    public IIndexCursor createSearchCursor() {
        return new InvertedIndexSearchCursor(searcher);
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        searcher.search((InvertedIndexSearchCursor) cursor, (InvertedIndexSearchPredicate) searchPred);
    }

}
