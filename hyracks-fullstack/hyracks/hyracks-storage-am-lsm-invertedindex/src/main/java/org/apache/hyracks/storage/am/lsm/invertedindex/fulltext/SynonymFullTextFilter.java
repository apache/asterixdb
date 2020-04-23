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

import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo;

import com.google.common.collect.ImmutableMap;

public class SynonymFullTextFilter extends AbstractFullTextFilter {
    private static final long serialVersionUID = 1L;

    ImmutableMap<String, String> synonymMap;

    public SynonymFullTextFilter(String name, ImmutableMap<String, String> synonymMap) {
        super(name, FullTextFilterType.STOPWORDS);
        this.synonymMap = synonymMap;
    }

    // Not implemented yet
    @Override
    public IToken processToken(TokenizerInfo.TokenizerType tokenizerType, IToken token) {
        return null;
    }
}
