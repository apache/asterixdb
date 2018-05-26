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

import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;

/**
 * Helper class for reading and writing job-gen parameters for access methods to
 * and from a list of function arguments, typically of an unnest-map.
 */
public class AccessMethodJobGenParams {
    private static final int NUM_PARAMS = 6;
    protected String indexName;
    protected IndexType indexType;
    protected String dataverseName;
    protected String datasetName;
    protected boolean retainInput;
    protected boolean requiresBroadcast;
    protected boolean isPrimaryIndex;

    public AccessMethodJobGenParams() {
        // Enable creation of an empty object and fill members using setters
    }

    public AccessMethodJobGenParams(String indexName, IndexType indexType, String dataverseName, String datasetName,
            boolean retainInput, boolean requiresBroadcast) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.retainInput = retainInput;
        this.requiresBroadcast = requiresBroadcast;
        this.isPrimaryIndex = datasetName.equals(indexName);
    }

    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        funcArgs.add(new MutableObject<>(AccessMethodUtils.createStringConstant(indexName)));
        funcArgs.add(new MutableObject<>(AccessMethodUtils.createInt32Constant(indexType.ordinal())));
        funcArgs.add(new MutableObject<>(AccessMethodUtils.createStringConstant(dataverseName)));
        funcArgs.add(new MutableObject<>(AccessMethodUtils.createStringConstant(datasetName)));
        funcArgs.add(new MutableObject<>(AccessMethodUtils.createBooleanConstant(retainInput)));
        funcArgs.add(new MutableObject<>(AccessMethodUtils.createBooleanConstant(requiresBroadcast)));
    }

    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        indexName = AccessMethodUtils.getStringConstant(funcArgs.get(0));
        indexType = IndexType.values()[AccessMethodUtils.getInt32Constant(funcArgs.get(1))];
        dataverseName = AccessMethodUtils.getStringConstant(funcArgs.get(2));
        datasetName = AccessMethodUtils.getStringConstant(funcArgs.get(3));
        retainInput = AccessMethodUtils.getBooleanConstant(funcArgs.get(4));
        requiresBroadcast = AccessMethodUtils.getBooleanConstant(funcArgs.get(5));
        isPrimaryIndex = datasetName.equals(indexName);
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public boolean getRetainInput() {
        return retainInput;
    }

    public boolean getRequiresBroadcast() {
        return requiresBroadcast;
    }

    protected void writeVarList(List<LogicalVariable> varList, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> numKeysRef =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(varList.size()))));
        funcArgs.add(numKeysRef);
        for (LogicalVariable keyVar : varList) {
            VariableReferenceExpression keyVarRef = new VariableReferenceExpression(keyVar);
            funcArgs.add(new MutableObject<>(keyVarRef));
        }
    }

    protected int readVarList(List<Mutable<ILogicalExpression>> funcArgs, int index, List<LogicalVariable> varList) {
        int numLowKeys = AccessMethodUtils.getInt32Constant(funcArgs.get(index));
        if (numLowKeys > 0) {
            for (int i = 0; i < numLowKeys; i++) {
                LogicalVariable var =
                        ((VariableReferenceExpression) funcArgs.get(index + 1 + i).getValue()).getVariableReference();
                varList.add(var);
            }
        }
        return index + numLowKeys + 1;
    }

    protected int getNumParams() {
        return NUM_PARAMS;
    }

    public boolean isPrimaryIndex() {
        return isPrimaryIndex;
    }
}
