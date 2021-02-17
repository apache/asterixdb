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

package org.apache.hyracks.storage.am.lsm.invertedindex.fulltext;

import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

import com.google.common.collect.ImmutableList;

// FullTextConfigEvaluator is a run-time evaluator while the IFullTextConfigDescriptor is a compile-time descriptor
//
// The descriptor is responsible for serialization (i.e. distributed from the CC to NC)
// and Metadata translator (i.e. be written to the metadata catalog)
// And the analyzer is to process the tokens in each NC at run-time
public class FullTextConfigEvaluator implements IFullTextConfigEvaluator {

    // name is not necessary for run-time token processing, however, let's keep this field for debugging purpose
    // The name of the evaluator is supposed to have the dataverse name and the name of the full-text config descriptor
    private final String name;
    // Due to the limitation of existing code (e.g. some call sites assume the input has a Asterix type tag while some don't),
    // we may need to replace the tokenizer on-the-fly in certain call sites.
    // So this field is not final
    private IBinaryTokenizer tokenizer;
    private final ImmutableList<IFullTextFilterEvaluator> filters;

    private IToken currentToken;
    private IToken nextToken;

    public FullTextConfigEvaluator(String name, TokenizerCategory tokenizerCategory,
            ImmutableList<IFullTextFilterEvaluator> filters) {
        this.name = name;
        this.filters = filters;

        switch (tokenizerCategory) {
            case WORD:
                // Currently, the tokenizer will be set later after the analyzer created
                // This is because the tokenizer logic is complex,
                // and we are already using a dedicated tokenizer factory to create tokenizer.
                // One tricky part of tokenizer is that it can be call-site specific, e.g. the string in some call-site
                // has the ATypeTag.String in the beginning of its byte array, and some doesn't,
                // so if we only know the category of the tokenizer, e.g. a WORD tokenizer,
                // we still cannot create a suitable tokenizer here as the tokenizer factory does.
                //
                // Finally we should get rid of the dedicated tokenizer factory and put its related logic
                // in the full-text descriptor and analyzer
                this.tokenizer = null;
                break;
            case NGRAM:
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public IBinaryTokenizer getTokenizer() {
        return tokenizer;
    }

    @Override
    public TokenizerCategory getTokenizerCategory() {
        return tokenizer.getTokenizerCategory();
    }

    @Override
    public void setTokenizer(IBinaryTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void reset(byte[] data, int start, int length) {
        currentToken = null;
        nextToken = null;
        tokenizer.reset(data, start, length);
    }

    @Override
    public IToken getToken() {
        return currentToken;
    }

    @Override
    public boolean hasNext() {
        if (nextToken != null) {
            return true;
        }

        while (tokenizer.hasNext()) {
            tokenizer.next();
            IToken candidateToken = tokenizer.getToken();
            for (IFullTextFilterEvaluator filter : filters) {
                // ToDo: Tokenizer of TokenizerType.List would return strings starting with the length,
                // e.g. 8database where 8 is the length
                // Should we let TokenizerType.List returns the same thing as TokenizerType.String to make things easier?
                // If so, we need to remove the length in the input string in all the call site of the tokenizer
                // Otherwise, filters need tokenizer.getTokenizerType to decide if they need to remove the length themselves
                candidateToken = filter.processToken(tokenizer.getTokenizerType(), candidateToken);
                // null means the token is removed, i.e. it is a stopword
                if (candidateToken == null) {
                    break;
                }
            }

            if (candidateToken != null) {
                nextToken = candidateToken;
                break;
            }
        }

        return nextToken != null;
    }

    @Override
    public void next() {
        currentToken = nextToken;
        nextToken = null;
    }

    @Override
    public int getTokensCount() {
        return tokenizer.getTokensCount();
    }

}
