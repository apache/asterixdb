/**
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

import java.security.InvalidParameterException;
import java.util.List;

import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class AbstractFullTextConfig implements IFullTextConfig {
    protected final String name;
    protected final TokenizerCategory tokenizerCategory;
    // By default, the tokenizer should be of the type DelimitedUTF8StringBinaryTokenizer
    // tokenizer needs be replaced on-the-fly when used in the ftcontains() function
    //
    // ftcontains() can take two types of input:
    // 1) string where a default DelimitedUTF8StringBinaryTokenizer is fine;
    // 2) a list of strings as input where we may need a AUnorderedListBinaryTokenizer or AOrderedListBinaryTokenizer

    // ToDo: wrap tokenizer and filters into a dedicated Java class so that at runtime the corresponding evaluator
    // doesn't care about usedByIndices
    protected IBinaryTokenizer tokenizer;
    protected ImmutableList<IFullTextFilter> filters;
    protected List<String> usedByIndices;
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected AbstractFullTextConfig(String name, TokenizerCategory tokenizerCategory,
            ImmutableList<IFullTextFilter> filters, List<String> usedByIndices) {
        this.name = name;
        this.tokenizerCategory = tokenizerCategory;
        this.filters = filters;
        this.usedByIndices = usedByIndices;

        switch (tokenizerCategory) {
            case WORD:
                // Similar to aqlStringTokenizerFactory which is in the upper Asterix layer
                // ToDo: should we move aqlStringTokenizerFactory so that it can be called in the Hyracks layer?
                // If so, we need to move ATypeTag to Hyracks as well
                // Another way to do so is to pass the tokenizer instance instead of the tokenizer category from Asterix to Hyracks
                // However, this may make the serializing part tricky because only the tokenizer category will be written to disk
                this.tokenizer =
                        new DelimitedUTF8StringBinaryTokenizerFactory(true, true, new UTF8WordTokenFactory((byte) 13, // ATypeTag.SERIALIZED_STRING_TYPE_TAG
                                (byte) 3) // ATypeTag.SERIALIZED_INT32_TYPE_TAG
                        ).createTokenizer();
                break;
            case NGRAM:
                throw new NotImplementedException();
                //this.tokenizer = new NGramUTF8StringBinaryTokenizerFactory(gramLength, usePrePost, true, true,
                //        new UTF8NGramTokenFactory());
            default:
                throw new InvalidParameterException();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FullTextEntityCategory getCategory() {
        return FullTextEntityCategory.CONFIG;
    }

    @Override
    public TokenizerCategory getTokenizerCategory() {
        return tokenizerCategory;
    }

    @Override
    public ImmutableList<IFullTextFilter> getFilters() {
        return filters;
    }

    @Override
    public List<String> getUsedByIndices() {
        return usedByIndices;
    }

    @Override
    public void addUsedByIndices(String indexName) {
        this.usedByIndices.add(indexName);
    }

    @Override
    public void setTokenizer(IBinaryTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public IBinaryTokenizer getTokenizer() {
        return this.tokenizer;
    }
}
