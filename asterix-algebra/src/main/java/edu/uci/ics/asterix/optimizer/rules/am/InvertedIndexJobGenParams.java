/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.optimizer.rules.am.InvertedIndexAccessMethod.SearchModifierType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

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

    public InvertedIndexJobGenParams() {
    }

    public InvertedIndexJobGenParams(String indexName, IndexType indexType, String dataverseName, String datasetName,
            boolean retainInput, boolean requiresBroadcast) {
        super(indexName, indexType, dataverseName, datasetName, retainInput, requiresBroadcast);
    }

    public void setSearchModifierType(SearchModifierType searchModifierType) {
        this.searchModifierType = searchModifierType;
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

    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.writeToFuncArgs(funcArgs);
        // Write search modifier type.
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createInt32Constant(searchModifierType
                .ordinal())));
        // Write similarity threshold.
        funcArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(similarityThreshold)));
        // Write search key type.
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createInt32Constant(searchKeyType
                .ordinal())));
        // Write key var list.
        writeVarList(keyVarList, funcArgs);
        // Write non-key var list.
        if (nonKeyVarList != null) {
            writeVarList(nonKeyVarList, funcArgs);
        }
    }

    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.readFromFuncArgs(funcArgs);
        int index = super.getNumParams();
        // Read search modifier type.
        int searchModifierOrdinal = AccessMethodUtils.getInt32Constant(funcArgs.get(index));
        searchModifierType = SearchModifierType.values()[searchModifierOrdinal];
        // Read similarity threshold. Concrete type depends on search modifier.
        similarityThreshold = ((AsterixConstantValue) ((ConstantExpression) funcArgs.get(index + 1).getValue())
                .getValue());
        // Read type of search key.
        int typeTagOrdinal = AccessMethodUtils.getInt32Constant(funcArgs.get(index + 2));
        searchKeyType = ATypeTag.values()[typeTagOrdinal];
        // Read key var list.
        keyVarList = new ArrayList<LogicalVariable>();
        readVarList(funcArgs, index + 3, keyVarList);
        // TODO: We could possibly simplify things if we did read the non-key var list here.
        // We don't need to read the non-key var list.
        nonKeyVarList = null;
    }

    public SearchModifierType getSearchModifierType() {
        return searchModifierType;
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