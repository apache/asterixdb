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

package org.apache.hyracks.storage.am.lsm.invertedindex.util;

import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluator;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

// TODO: We can possibly avoid copying the data into a new tuple here.
public class PartitionedInvertedIndexTokenizingTupleIterator extends InvertedIndexTokenizingTupleIterator {

    protected short numTokens = 0;

    public PartitionedInvertedIndexTokenizingTupleIterator(int tokensFieldCount, int invListFieldCount,
            IBinaryTokenizer tokenizer, IFullTextConfigEvaluator fullTextConfigEvaluator) {
        super(tokensFieldCount, invListFieldCount, tokenizer, fullTextConfigEvaluator);
    }

    @Override
    public void reset(ITupleReference inputTuple) {
        super.reset(inputTuple);
        // Run through the tokenizer once to get the total number of tokens.
        numTokens = 0;
        while (fullTextConfigEvaluator.hasNext()) {
            fullTextConfigEvaluator.next();
            numTokens++;
        }
        super.reset(inputTuple);
    }

    @Override
    public void next() throws HyracksDataException {
        fullTextConfigEvaluator.next();
        IToken token = fullTextConfigEvaluator.getToken();

        tupleBuilder.reset();
        try {
            // Add token field.
            token.serializeToken(tupleBuilder.getFieldData());

            // Different from super.next(): here we write the numTokens
            tupleBuilder.addFieldEndOffset();
            // Add field with number of tokens.
            tupleBuilder.getDataOutput().writeShort(numTokens);
            tupleBuilder.addFieldEndOffset();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

        // Add inverted-list element fields.
        for (int i = 0; i < invListFieldCount; i++) {
            tupleBuilder.addField(inputTuple.getFieldData(i + 1), inputTuple.getFieldStart(i + 1),
                    inputTuple.getFieldLength(i + 1));
        }
        // Reset tuple reference for insert operation.
        tupleReference.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    public short getNumTokens() {
        return numTokens;
    }
}
