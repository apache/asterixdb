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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class FullTextConfigEvaluatorFactory implements IFullTextConfigEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final TokenizerCategory tokenizerCategory;
    private final ImmutableList<IFullTextFilterEvaluatorFactory> filters;

    public FullTextConfigEvaluatorFactory(String name, TokenizerCategory tokenizerCategory,
            ImmutableList<IFullTextFilterEvaluatorFactory> filters) {
        this.name = name;
        this.tokenizerCategory = tokenizerCategory;
        this.filters = filters;
    }

    @Override
    public IFullTextConfigEvaluator createFullTextConfigEvaluator() {
        ImmutableList.Builder<IFullTextFilterEvaluator> filterEvaluatorsBuilder = ImmutableList.builder();
        for (IFullTextFilterEvaluatorFactory factory : filters) {
            filterEvaluatorsBuilder.add(factory.createFullTextFilterEvaluator());
        }
        return new FullTextConfigEvaluator(name, tokenizerCategory, filterEvaluatorsBuilder.build());
    }

    public static IFullTextConfigEvaluatorFactory getDefaultFactory() {
        return new FullTextConfigEvaluatorFactory("default_config_evaluator_factory", TokenizerCategory.WORD,
                ImmutableList.of());
    }

    private static final String FIELD_NAME = "name";
    private static final String FIELD_TOKENIZER_CATEGORY = "tokenizerCategory";
    private static final String FIELD_FILTERS = "filters";
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put(FIELD_NAME, name);
        json.put(FIELD_TOKENIZER_CATEGORY, tokenizerCategory.toString());

        final ArrayNode filterArray = OBJECT_MAPPER.createArrayNode();
        for (IFullTextFilterEvaluatorFactory filter : filters) {
            filterArray.add(filter.toJson(registry));
        }
        json.set(FIELD_FILTERS, filterArray);

        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final String name = json.get(FIELD_NAME).asText();
        final String tokenizerCategoryStr = json.get(FIELD_TOKENIZER_CATEGORY).asText();
        TokenizerCategory tc = TokenizerCategory.getEnumIgnoreCase(tokenizerCategoryStr);

        ArrayNode filtersJsonNode = (ArrayNode) json.get(FIELD_FILTERS);
        ImmutableList.Builder<IFullTextFilterEvaluatorFactory> filtersBuilder = ImmutableList.builder();
        for (int i = 0; i < filtersJsonNode.size(); i++) {
            filtersBuilder.add((IFullTextFilterEvaluatorFactory) registry.deserialize(filtersJsonNode.get(i)));
        }
        return new FullTextConfigEvaluatorFactory(name, tc, filtersBuilder.build());
    }
}
