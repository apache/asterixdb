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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.AbstractInvertedIndexInsertTest;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.NGramUTF8StringBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8NGramTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestUtils;

public class InMemoryBTreeInvertedIndexInsertTest extends AbstractInvertedIndexInsertTest {

    @Override
    protected void setTokenizer() {
        ITokenFactory tokenFactory = new UTF8NGramTokenFactory();
        tokenizer = new NGramUTF8StringBinaryTokenizer(3, false, true, false, tokenFactory);
//        ITokenFactory tokenFactory = new UTF8WordTokenFactory();
//        tokenizer = new DelimitedUTF8StringBinaryTokenizer(true, false, tokenFactory);
    }

    @Override
    protected void setInvertedIndex() throws HyracksDataException {
        invertedIndex = InvertedIndexTestUtils.createInMemoryInvertedIndex(harness, tokenizer);
        invertedIndex.create();
        invertedIndex.activate();
        invertedIndex.create(harness.getFileId());
        invertedIndex.open(harness.getFileId());
    }

    @Override
    protected void setLogger() {
        LOGGER = Logger.getLogger(InMemoryBTreeInvertedIndexInsertTest.class.getName());
    }

    @Override
    protected void setRandom() {
        random = true;
    }

}
