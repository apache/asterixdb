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
package org.apache.asterix.formats.nontagged;

import org.apache.asterix.dataflow.data.common.AListElementTokenFactory;
import org.apache.asterix.dataflow.data.common.AOrderedListBinaryTokenizerFactory;
import org.apache.asterix.dataflow.data.common.AUnorderedListBinaryTokenizerFactory;
import org.apache.asterix.dataflow.data.common.IBinaryTokenizerFactoryProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.HashedUTF8WordTokenFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8NGramTokenFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;

public class AqlBinaryTokenizerFactoryProvider implements IBinaryTokenizerFactoryProvider {

    public static final AqlBinaryTokenizerFactoryProvider INSTANCE = new AqlBinaryTokenizerFactoryProvider();

    private static final IBinaryTokenizerFactory aqlStringTokenizer = new DelimitedUTF8StringBinaryTokenizerFactory(
            true, true, new UTF8WordTokenFactory(ATypeTag.STRING.serialize(), ATypeTag.INT32.serialize()));

    private static final IBinaryTokenizerFactory aqlHashingStringTokenizer = new DelimitedUTF8StringBinaryTokenizerFactory(
            true, true, new HashedUTF8WordTokenFactory(ATypeTag.INT32.serialize(), ATypeTag.INT32.serialize()));

    private static final IBinaryTokenizerFactory orderedListTokenizer = new AOrderedListBinaryTokenizerFactory(
            new AListElementTokenFactory());

    private static final IBinaryTokenizerFactory unorderedListTokenizer = new AUnorderedListBinaryTokenizerFactory(
            new AListElementTokenFactory());

    @Override
    public IBinaryTokenizerFactory getWordTokenizerFactory(ATypeTag typeTag, boolean hashedTokens) {
        switch (typeTag) {
            case STRING: {
                if (hashedTokens) {
                    return aqlHashingStringTokenizer;
                } else {
                    return aqlStringTokenizer;
                }
            }
            case ORDEREDLIST: {
                return orderedListTokenizer;
            }
            case UNORDEREDLIST: {
                return unorderedListTokenizer;
            }
            default: {
                return null;
            }
        }
    }

    @Override
    public IBinaryTokenizerFactory getNGramTokenizerFactory(ATypeTag typeTag, int gramLength, boolean usePrePost,
            boolean hashedTokens) {
        switch (typeTag) {
            case STRING: {
                if (hashedTokens) {
                    return null;
                } else {
                    return new NGramUTF8StringBinaryTokenizerFactory(gramLength, usePrePost, true, true,
                            new UTF8NGramTokenFactory(ATypeTag.STRING.serialize(), ATypeTag.INT32.serialize()));
                }
            }
            case ORDEREDLIST: {
                return orderedListTokenizer;
            }
            case UNORDEREDLIST: {
                return unorderedListTokenizer;
            }
            default: {
                return null;
            }
        }
    }
}
