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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util;

import java.util.Collection;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.InvertedIndexException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

@SuppressWarnings("rawtypes")
public class InvertedIndexTestContext extends OrderedIndexTestContext {

    public static enum InvertedIndexType {
        INMEMORY,
        ONDISK,
        LSM
    };

    protected IBinaryComparatorFactory[] allCmpFactories;

    public InvertedIndexTestContext(ISerializerDeserializer[] fieldSerdes, IIndex index) {
        super(fieldSerdes, index);
    }

    @Override
    public int getKeyFieldCount() {
        return fieldSerdes.length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        if (allCmpFactories == null) {
            // Concatenate token and inv-list comparators.
            IInvertedIndex invIndex = (IInvertedIndex) index;
            IBinaryComparatorFactory[] tokenCmpFactories = invIndex.getTokenCmpFactories();
            IBinaryComparatorFactory[] invListCmpFactories = invIndex.getInvListCmpFactories();
            int totalCmpCount = tokenCmpFactories.length + invListCmpFactories.length;
            allCmpFactories = new IBinaryComparatorFactory[totalCmpCount];
            for (int i = 0; i < tokenCmpFactories.length; i++) {
                allCmpFactories[i] = tokenCmpFactories[i];
            }
            for (int i = 0; i < invListCmpFactories.length; i++) {
                allCmpFactories[i + tokenCmpFactories.length] = invListCmpFactories[i];
            }
        }
        return allCmpFactories;
    }

    public static InvertedIndexTestContext create(IBufferCache bufferCache, IFreePageManager freePageManager,
            IFileMapProvider fileMapProvider, FileReference invListsFile, ISerializerDeserializer[] fieldSerdes,
            int tokenFieldCount, IBinaryTokenizerFactory tokenizerFactory, InvertedIndexType invIndexType)
            throws Exception {
        ITypeTraits[] allTypeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] allCmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes,
                fieldSerdes.length);
        // Set token type traits and comparators.
        ITypeTraits[] tokenTypeTraits = new ITypeTraits[tokenFieldCount];
        IBinaryComparatorFactory[] tokenCmpFactories = new IBinaryComparatorFactory[tokenFieldCount];
        for (int i = 0; i < tokenTypeTraits.length; i++) {
            tokenTypeTraits[i] = allTypeTraits[i];
            tokenCmpFactories[i] = allCmpFactories[i];
        }
        // Set inverted-list element type traits and comparators.
        int invListFieldCount = fieldSerdes.length - tokenFieldCount;
        ITypeTraits[] invListTypeTraits = new ITypeTraits[invListFieldCount];
        IBinaryComparatorFactory[] invListCmpFactories = new IBinaryComparatorFactory[invListFieldCount];
        for (int i = 0; i < invListTypeTraits.length; i++) {
            invListTypeTraits[i] = allTypeTraits[i + tokenFieldCount];
            invListCmpFactories[i] = allCmpFactories[i + tokenFieldCount];
        }
        // Create index and test context.
        IInvertedIndex invIndex;
        switch (invIndexType) {
            case INMEMORY: {
                invIndex = InvertedIndexUtils.createInMemoryBTreeInvertedindex(bufferCache, freePageManager,
                        invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory);
                break;
            }
            case ONDISK: {
                invIndex = InvertedIndexUtils.createOnDiskInvertedIndex(bufferCache, fileMapProvider,
                        invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, invListsFile);
                break;
            }
            default: {
                throw new InvertedIndexException("Unknow inverted-index type '" + invIndexType + "'.");
            }
        }
        InvertedIndexTestContext testCtx = new InvertedIndexTestContext(fieldSerdes, invIndex);
        return testCtx;
    }

    @Override
    public void upsertCheckTuple(CheckTuple checkTuple, Collection<CheckTuple> checkTuples) {
        throw new UnsupportedOperationException("Upsert not supported by inverted index.");
    }
}
