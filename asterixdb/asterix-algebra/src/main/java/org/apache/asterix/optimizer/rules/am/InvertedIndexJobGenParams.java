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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.optimizer.rules.am.InvertedIndexAccessMethod.SearchModifierType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

/**
 * Helper class for reading and writing job-gen parameters for RTree access methods to
 * and from a list of function arguments, typically of an unnest-map.
 */
public class InvertedIndexJobGenParams extends AccessMethodJobGenParams {

    protected SearchModifierType searchModifierType;
    protected IAlgebricksConstantValue similarityThreshold;
    protected ATypeTag searchKeyType;
    protected List<LogicalVariable> keyVarList;
    protected List<LogicalVariable> nonKeyVarList;
    // TODO: Currently, we don't have positional information in an inverted index.
    // Thus, we can't support the phrase search yet. So, for the full-text search,
    // if a query predicate contains a phrase, we need to generate an exception.
    // The following variable serves this purpose. i.e. Checks whether the query is a full-text search query or not.
    protected boolean isFullTextSearchQuery = false;
    protected static final int SEARCH_MODIFIER_INDEX = 0;
    protected static final int SIM_THRESHOLD_INDEX = 1;
    protected static final int SEARCH_KEY_TYPE_INDEX = 2;
    protected static final int IS_FULLTEXT_SEARCH_INDEX = 3;
    protected static final int KEY_VAR_INDEX = 4;

    public InvertedIndexJobGenParams() {
    }

    public InvertedIndexJobGenParams(String indexName, IndexType indexType, String dataverseName, String datasetName,
            boolean retainInput, boolean requiresBroadcast) {
        super(indexName, indexType, dataverseName, datasetName, retainInput, requiresBroadcast);
    }

    public void setSearchModifierType(SearchModifierType searchModifierType) {
        this.searchModifierType = searchModifierType;
    }

    public void setIsFullTextSearch(boolean isFullTextSearchQuery) {
        this.isFullTextSearchQuery = isFullTextSearchQuery;
    }

    public void setSimilarityThreshold(IAlgebricksConstantValue similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }

    public void setSearchKeyType(ATypeTag searchKeyType) {
        this.searchKeyType = searchKeyType;
    }

    public void setKeyVarList(List<LogicalVariable> keyVarList) {
        this.keyVarList = keyVarList;
    }

    @Override
    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.writeToFuncArgs(funcArgs);
        // Write search modifier type.
        funcArgs.add(new MutableObject<ILogicalExpression>(
                AccessMethodUtils.createInt32Constant(searchModifierType.ordinal())));
        // Write similarity threshold.
        funcArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(similarityThreshold)));
        // Write search key type.
        funcArgs.add(
                new MutableObject<ILogicalExpression>(AccessMethodUtils.createInt32Constant(searchKeyType.ordinal())));
        // Write full-text search information.
        funcArgs.add(
                new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(isFullTextSearchQuery)));
        // Write key var list.
        writeVarList(keyVarList, funcArgs);
        // Write non-key var list.
        if (nonKeyVarList != null) {
            writeVarList(nonKeyVarList, funcArgs);
        }
    }

    @Override
    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.readFromFuncArgs(funcArgs);
        int index = super.getNumParams();
        // Read search modifier type.
        int searchModifierOrdinal = AccessMethodUtils.getInt32Constant(funcArgs.get(index + SEARCH_MODIFIER_INDEX));
        searchModifierType = SearchModifierType.values()[searchModifierOrdinal];
        // Read similarity threshold. Concrete type depends on search modifier.
        similarityThreshold = ((ConstantExpression) funcArgs.get(index + SIM_THRESHOLD_INDEX).getValue()).getValue();
        // Read type of search key.
        int typeTagOrdinal = AccessMethodUtils.getInt32Constant(funcArgs.get(index + SEARCH_KEY_TYPE_INDEX));
        searchKeyType = ATypeTag.values()[typeTagOrdinal];
        // Read full-text search information.
        isFullTextSearchQuery = AccessMethodUtils.getBooleanConstant(funcArgs.get(index + IS_FULLTEXT_SEARCH_INDEX));
        // Read key var list.
        keyVarList = new ArrayList<>();
        readVarList(funcArgs, index + KEY_VAR_INDEX, keyVarList);
        // TODO: We could possibly simplify things if we did read the non-key var list here.
        // We don't need to read the non-key var list.
        nonKeyVarList = null;
    }

    public SearchModifierType getSearchModifierType() {
        return searchModifierType;
    }

    public boolean getIsFullTextSearch() {
        return isFullTextSearchQuery;
    }

    public IAlgebricksConstantValue getSimilarityThreshold() {
        return similarityThreshold;
    }

    public ATypeTag getSearchKeyType() {
        return searchKeyType;
    }

    public List<LogicalVariable> getKeyVarList() {
        return keyVarList;
    }

    public List<LogicalVariable> getNonKeyVarList() {
        return nonKeyVarList;
    }
}
