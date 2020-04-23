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

import java.util.List;

import org.apache.commons.lang3.EnumUtils;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

import com.google.common.collect.ImmutableList;

public interface IFullTextConfig extends IFullTextEntity {
    // case-insensitive
    String FIELD_NAME_TOKENIZER = "tokenizer";
    String FIELD_NAME_FILTER_PIPELINE = "filter_pipeline";

    enum TokenizerCategory {
        NGRAM,
        WORD;

        public static TokenizerCategory fromString(String str) {
            return EnumUtils.getEnumIgnoreCase(TokenizerCategory.class, str);
        }
    }

    TokenizerCategory getTokenizerCategory();

    void setTokenizer(IBinaryTokenizer tokenizer);

    IBinaryTokenizer getTokenizer();

    ImmutableList<IFullTextFilter> getFilters();

    List<String> getUsedByIndices();

    void addUsedByIndices(String indexName);

    void reset(byte[] data, int start, int length);

    IToken getToken();

    boolean hasNext();

    void next();

    // Get the total number of tokens
    // Currently, it returns the number of tokens in the original text, that means stopwords are still counted
    short getTokensCount();
}
