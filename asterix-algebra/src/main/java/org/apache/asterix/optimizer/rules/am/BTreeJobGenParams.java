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
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;

/**
 * Helper class for reading and writing job-gen parameters for BTree access methods to
 * and from a list of function arguments, typically of an unnest-map.
 */
public class BTreeJobGenParams extends AccessMethodJobGenParams {

    protected List<LogicalVariable> lowKeyVarList;
    protected List<LogicalVariable> highKeyVarList;

    protected boolean lowKeyInclusive;
    protected boolean highKeyInclusive;
    protected boolean isEqCondition;

    public BTreeJobGenParams() {
        super();
    }

    public BTreeJobGenParams(String indexName, IndexType indexType, String dataverseName, String datasetName,
            boolean retainInput, boolean requiresBroadcast) {
        super(indexName, indexType, dataverseName, datasetName, retainInput, requiresBroadcast);
    }

    public void setLowKeyVarList(List<LogicalVariable> keyVarList, int startIndex, int numKeys) {
        lowKeyVarList = new ArrayList<LogicalVariable>(numKeys);
        setKeyVarList(keyVarList, lowKeyVarList, startIndex, numKeys);
    }

    public void setHighKeyVarList(List<LogicalVariable> keyVarList, int startIndex, int numKeys) {
        highKeyVarList = new ArrayList<LogicalVariable>(numKeys);
        setKeyVarList(keyVarList, highKeyVarList, startIndex, numKeys);
    }

    private void setKeyVarList(List<LogicalVariable> src, List<LogicalVariable> dest, int startIndex, int numKeys) {
        for (int i = 0; i < numKeys; i++) {
            dest.add(src.get(startIndex + i));
        }
    }

    public void setLowKeyInclusive(boolean lowKeyInclusive) {
        this.lowKeyInclusive = lowKeyInclusive;
    }

    public void setHighKeyInclusive(boolean highKeyInclusive) {
        this.highKeyInclusive = highKeyInclusive;
    }

    public void setIsEqCondition(boolean isEqConsition) {
        this.isEqCondition = isEqConsition;
    }

    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.writeToFuncArgs(funcArgs);
        writeVarList(lowKeyVarList, funcArgs);
        writeVarList(highKeyVarList, funcArgs);
        writeBoolean(lowKeyInclusive, funcArgs);
        writeBoolean(highKeyInclusive, funcArgs);
        writeBoolean(isEqCondition, funcArgs);
    }

    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.readFromFuncArgs(funcArgs);
        int index = super.getNumParams();
        lowKeyVarList = new ArrayList<LogicalVariable>();
        highKeyVarList = new ArrayList<LogicalVariable>();
        int nextIndex = readVarList(funcArgs, index, lowKeyVarList);
        nextIndex = readVarList(funcArgs, nextIndex, highKeyVarList);
        nextIndex = readKeyInclusives(funcArgs, nextIndex);
        readIsEqCondition(funcArgs, nextIndex);
    }

    private int readKeyInclusives(List<Mutable<ILogicalExpression>> funcArgs, int index) {
        lowKeyInclusive = ((ConstantExpression) funcArgs.get(index).getValue()).getValue().isTrue();
        // Read the next function argument at index + 1.
        highKeyInclusive = ((ConstantExpression) funcArgs.get(index + 1).getValue()).getValue().isTrue();
        // We have read two of the function arguments, so the next index is at index + 2.
        return index + 2;
    }

    private void readIsEqCondition(List<Mutable<ILogicalExpression>> funcArgs, int index) {
        isEqCondition = ((ConstantExpression) funcArgs.get(index).getValue()).getValue().isTrue();
    }

    private void writeBoolean(boolean val, List<Mutable<ILogicalExpression>> funcArgs) {
        ILogicalExpression keyExpr = val ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        funcArgs.add(new MutableObject<ILogicalExpression>(keyExpr));
    }

    public List<LogicalVariable> getLowKeyVarList() {
        return lowKeyVarList;
    }

    public List<LogicalVariable> getHighKeyVarList() {
        return highKeyVarList;
    }

    public boolean isEqCondition() {
        return isEqCondition;
    }

    public boolean isLowKeyInclusive() {
        return lowKeyInclusive;
    }

    public boolean isHighKeyInclusive() {
        return highKeyInclusive;
    }
}
