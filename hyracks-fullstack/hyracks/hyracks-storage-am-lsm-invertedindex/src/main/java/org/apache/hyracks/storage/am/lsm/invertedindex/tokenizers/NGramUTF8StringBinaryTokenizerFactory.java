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

package org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NGramUTF8StringBinaryTokenizerFactory implements IBinaryTokenizerFactory {

    private static final long serialVersionUID = 1L;
    private final int gramLength;
    private final boolean usePrePost;
    private final boolean ignoreTokenCount;
    private final boolean sourceHasTypeTag;
    private final ITokenFactory tokenFactory;

    public NGramUTF8StringBinaryTokenizerFactory(int gramLength, boolean usePrePost, boolean ignoreTokenCount,
            boolean sourceHasTypeTag, ITokenFactory tokenFactory) {
        this.gramLength = gramLength;
        this.usePrePost = usePrePost;
        this.ignoreTokenCount = ignoreTokenCount;
        this.sourceHasTypeTag = sourceHasTypeTag;
        this.tokenFactory = tokenFactory;
    }

    @Override
    public IBinaryTokenizer createTokenizer() {
        return new NGramUTF8StringBinaryTokenizer(gramLength, usePrePost, ignoreTokenCount, sourceHasTypeTag,
                tokenFactory);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.set("tokenFactory", tokenFactory.toJson(registry));
        json.put("gramLength", gramLength);
        json.put("usePrePost", usePrePost);
        json.put("ignoreTokenCount", ignoreTokenCount);
        json.put("sourceHasTypeTag", sourceHasTypeTag);
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final ITokenFactory tokenFactory = (ITokenFactory) registry.deserialize(json.get("tokenFactory"));
        final int gramLength = json.get("gramLength").asInt();
        final boolean usePrePost = json.get("usePrePost").asBoolean();
        final boolean ignoreTokenCount = json.get("ignoreTokenCount").asBoolean();
        final boolean sourceHasTypeTag = json.get("sourceHasTypeTag").asBoolean();
        return new NGramUTF8StringBinaryTokenizerFactory(gramLength, usePrePost, ignoreTokenCount, sourceHasTypeTag,
                tokenFactory);
    }

}
