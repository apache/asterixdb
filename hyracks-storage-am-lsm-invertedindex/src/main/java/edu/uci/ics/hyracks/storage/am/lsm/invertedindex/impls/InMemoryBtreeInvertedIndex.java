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

import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class InMemoryBtreeInvertedIndex implements IIndex {

    private final BTree btree;
    private final IIndexAccessor btreeAccessor;
//    private final ITypeTraits[] tokenTypeTraits;
//    private final IBinaryComparatorFactory[] tokenCmpFactories;
    private final ITypeTraits[] invertedListTypeTraits;
    private final IBinaryComparatorFactory[] invertedListCmpFactories;
    private final IBinaryTokenizer tokenizer;
    private final int numTokenFields;

    private final ArrayTupleBuilder btreeTupleBuilder;
    private final ArrayTupleReference btreeTupleReference;

//    public InMemoryBtreeInvertedIndex(IBufferCache inMemBufferCache, IFreePageManager inMemFreePageManager,
//            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
//            ITypeTraits[] invertedListTypeTraits, IBinaryComparatorFactory[] invertedListCmpFactories,
//            IBinaryTokenizer tokenizer) {
//        this.tokenTypeTraits = tokenTypeTraits;
//        this.tokenCmpFactories = tokenCmpFactories;
//        this.invertedListTypeTraits = invertedListTypeTraits;
//        this.invertedListCmpFactories = invertedListCmpFactories;
//        this.tokenizer = tokenizer;
//        
//        this.btreeTupleBuilder = new ArrayTupleBuilder(btree.getFieldCount());
//        this.btreeTupleReference = new ArrayTupleReference();
//    }

    public InMemoryBtreeInvertedIndex(BTree btree, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, IBinaryTokenizer tokenizer) {
        // membufcache, tokenCmpFactories, memfpmgr, tokentypetraits
        //
        // IBufferCache bufferCache, int fieldCount, IBinaryComparatorFactory[] cmpFactories, IFreePageManager freePageManager,
        // ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory
        this.btree = btree;
        this.btreeAccessor = btree.createAccessor();
        this.invertedListTypeTraits = invListTypeTraits;
        this.invertedListCmpFactories = invListCmpFactories;
        this.tokenizer = tokenizer;
        this.numTokenFields = btree.getComparatorFactories().length - invListCmpFactories.length;

        // To generate in-memory BTree tuples 
        this.btreeTupleBuilder = new ArrayTupleBuilder(btree.getFieldCount());
        this.btreeTupleReference = new ArrayTupleReference();
    }

    @Override
	public void create() throws HyracksDataException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() throws HyracksDataException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clear() throws HyracksDataException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() throws HyracksDataException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void destroy() throws HyracksDataException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IIndexAccessor createAccessor(
			IModificationOperationCallback modificationCallback,
			ISearchOperationCallback searchCallback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void validate() throws HyracksDataException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getInMemorySize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public IIndexBulkLoader createBulkLoader(float fillFactor,
			boolean verifyInput) throws IndexException {
		// TODO Auto-generated method stub
		return null;
	}
    
    @Override
    public void open(int fileId) {
        btree.open(fileId);
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        btree.create(indexFileId);
    }

    @Override
    public void close() {
        btree.close();
    }

    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            IndexException {
        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) ictx;

        //Tuple --> |Field1|Field2| ... |FieldN|doc-id|
        //Each field represents a document and doc-id always comes at the last field.
        //parse document
        //create a list of (term,doc-id)
        //sort the list in the order of term
        //insert a pair of (term, doc-id) into in-memory BTree until to the end of the list.

        for (int i = 0; i < numTokenFields; i++) {
            tokenizer.reset(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            while (tokenizer.hasNext()) {
                tokenizer.next();
                IToken token = tokenizer.getToken();
                btreeTupleBuilder.reset();

                try {
                    token.serializeToken(btreeTupleBuilder.getDataOutput());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                btreeTupleBuilder.addFieldEndOffset();

                // This doesn't work for some reason.
                //  btreeTupleBuilder.addField(token.getData(), token.getStart(), token.getTokenLength());

                btreeTupleBuilder.addField(tuple.getFieldData(0), tuple.getFieldStart(1), tuple.getFieldLength(1));
                btreeTupleReference.reset(btreeTupleBuilder.getFieldEndOffsets(), btreeTupleBuilder.getByteArray());

                try {
                    btreeAccessor.insert(btreeTupleReference);
                } catch (BTreeDuplicateKeyException e) {
                    // Consciously ignoring... guarantees uniqueness!
                    // When duplication occurs, the current insert can be simply ignored 
                    // since the current inverted list stores doc-id only.
                    // TODO 
                    // We may work around this duplication issue by pre-processing the inserted document.
                    // This pre-processing will generate only unique <term, doc-id> pair for each document.
                    // Therefore there will be no duplication in in-memory BTree.    
                }
            }
        }
        return true;
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        return new InMemoryBtreeInvertedListCursor(btree, invertedListTypeTraits);
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference tupleReference)
            throws HyracksDataException, IndexException {
        InMemoryBtreeInvertedListCursor inMemListCursor = (InMemoryBtreeInvertedListCursor) listCursor;
        inMemListCursor.reset(tupleReference);
    }

    @Override
    public IIndexAccessor createAccessor() {
        return new InMemoryBtreeInvertedIndexAccessor(this, new LSMInvertedIndexOpContext(this), tokenizer);
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws IndexException, HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IBufferCache getBufferCache() {
        return null;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.INVERTED;
    }

    @Override
    public IBinaryComparatorFactory[] getInvListElementCmpFactories() {
        return invertedListCmpFactories;
    }

    @Override
    public ITypeTraits[] getTypeTraits() {
        return invertedListTypeTraits;
    }

    public BTree getBTree() {
        return btree;
    }
}
