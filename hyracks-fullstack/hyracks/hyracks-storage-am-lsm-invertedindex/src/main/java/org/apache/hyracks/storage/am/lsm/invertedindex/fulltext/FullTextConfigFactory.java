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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class FullTextConfigFactory implements IFullTextConfigFactory {
    private static final long serialVersionUID = 1L;

    private final IFullTextConfig config;

    public FullTextConfigFactory(IFullTextConfig config) {
        this.config = config;
    }

    @Override
    public IFullTextConfig createFullTextConfig() {
        if (config == null) {
            // If not specified, use the the default full-text config
            // Note that though the tokenizer here is of category Word, it may be replaced by a NGram tokenizer at run time
            //     for NGram index.
            return new FullTextConfig(FullTextConfig.DEFAULT_FULL_TEXT_CONFIG_NAME,
                    IFullTextConfig.TokenizerCategory.WORD, ImmutableList.of());
        }

        // All the components in the full-text config can be reused except the tokenizer.
        // The same config may be used in different places at the same time
        // For example, in ftcontains() the left expression and right expression need to be proceeded by two full-text configs
        // with the same filters but dedicated tokenizers
        return new FullTextConfig(config.getName(), config.getTokenizerCategory(), config.getFilters(),
                config.getUsedByIndices());
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        // ToDo: add tokenizerFactory into FullTextConfigFactory so a new tokenizer can be generated on-the-fly
        // rather than pass a tokenizer from the upper-layer caller to the full-text config
        if (config != null) {
            json.set("fullTextConfig", config.toJson(registry));
        } else {
            json.set("fullTextConfig", null);
        }
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        if (json.get("fullTextConfig").isNull()) {
            return null;
        }

        final IFullTextConfig config = (IFullTextConfig) registry.deserialize(json.get("fullTextConfig"));
        return new FullTextConfigFactory(config);
    }
}
