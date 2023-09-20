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

package org.apache.asterix.runtime.fulltext;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextConfigEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilterEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.TokenizerCategory;

import com.google.common.collect.ImmutableList;

// Full-text config that contains a tokenizer (e.g. a WORK tokenizer) and multiple full-text filters (e.g. stopwords filter)
// to tokenize and process tokens of full-text documents
// When running the ftcontains() function, the full-text config can be used with or without a full-text index
public class FullTextConfigDescriptor implements IFullTextConfigDescriptor {
    private static final long serialVersionUID = 2L;

    private final String databaseName;
    private final DataverseName dataverseName;
    private final String name;
    private final TokenizerCategory tokenizerCategory;
    private final ImmutableList<String> filterNames;

    public FullTextConfigDescriptor(String databaseName, DataverseName dataverseName, String name,
            TokenizerCategory tokenizerCategory, ImmutableList<String> filterNames) {
        //TODO(DB): database name should not be null
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.name = name;
        this.tokenizerCategory = tokenizerCategory;
        this.filterNames = filterNames;
    }

    // This built-in default full-text config will be used only when no full-text config is specified by the user.
    // Note that the default ft config descriptor is not stored in metadata catalog,
    // and if we are trying to get a full-text config descriptor with a name of null or empty string,
    // the metadata manager will return this default full-text config without looking into the metadata catalog
    // In this way we avoid the edge cases to insert or delete the default config in the metadata catalog
    public static FullTextConfigDescriptor getDefaultFullTextConfig() {
        return new FullTextConfigDescriptor(null, null, null, TokenizerCategory.WORD, ImmutableList.of());
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    @Override
    public String getName() {
        return name;
    }

    // We need to exclude the full-text filter descriptors from the full-text config because both of them
    // would be in the metadata cache, that means they should be immutable to guarantee consistency
    // So we decide to let the caller to be responsible for fetching the filter descriptors from metadata,
    // and pass the filters as an argument here
    //
    // Use the util function org.apache.asterix.metadata.utils.FullTextUtil.fetchFilterAndCreateConfigEvaluator()
    // to fetch filters according to the filter names and create full-text config evaluator
    @Override
    public IFullTextConfigEvaluatorFactory createEvaluatorFactory(
            ImmutableList<AbstractFullTextFilterDescriptor> filterDescriptors) {
        ImmutableList.Builder<IFullTextFilterEvaluatorFactory> filtersBuilder = new ImmutableList.Builder<>();
        for (IFullTextFilterDescriptor filterDescriptor : filterDescriptors) {
            filtersBuilder.add(filterDescriptor.createEvaluatorFactory());
        }

        return new FullTextConfigEvaluatorFactory(name, tokenizerCategory, filtersBuilder.build());
    }

    @Override
    public TokenizerCategory getTokenizerCategory() {
        return tokenizerCategory;
    }

    @Override
    public ImmutableList<String> getFilterNames() {
        return filterNames;
    }
}
