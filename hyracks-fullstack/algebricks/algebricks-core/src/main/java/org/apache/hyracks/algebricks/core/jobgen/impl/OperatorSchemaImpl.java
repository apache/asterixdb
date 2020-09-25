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
package org.apache.hyracks.algebricks.core.jobgen.impl;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class OperatorSchemaImpl implements IOperatorSchema {

    private final Object2IntMap<LogicalVariable> varMap;

    private final List<LogicalVariable> varList;

    public OperatorSchemaImpl() {
        varMap = new Object2IntOpenHashMap<>();
        varList = new ArrayList<>();
    }

    @Override
    public void addAllVariables(IOperatorSchema source) {
        for (LogicalVariable v : source) {
            addVariable(v);
        }
    }

    @Override
    public void addAllNewVariables(IOperatorSchema source) {
        // source schema can contain the same variable more than once,
        // we need to add it as many times if this variable is new.
        int sourceLen = source.getSize();
        BitSet newVarIdxs = null;
        for (int i = 0; i < sourceLen; i++) {
            LogicalVariable v = source.getVariable(i);
            if (!varMap.containsKey(v)) {
                addVariable(v);
                if (newVarIdxs == null) {
                    newVarIdxs = new BitSet(sourceLen);
                }
                newVarIdxs.set(i);
            } else {
                // if varMap contains this variable then we need to differentiate between two cases:
                // 1. the variable was present before this method was called, therefore it's not new
                // 2. the variable was not present before this method was called, but was added earlier in this loop
                //    (i.e. it is a duplicate entry of the same variable in the source schema)
                //    in this case it is considered new and we need to add it to varMap
                if (newVarIdxs != null) {
                    for (int j = newVarIdxs.nextSetBit(0); j >= 0; j = newVarIdxs.nextSetBit(j + 1)) {
                        if (v.equals(source.getVariable(j))) {
                            // this variable was previously added as 'new'
                            // we need to add a duplicate entry for this variable
                            addVariable(v);
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    public int addVariable(LogicalVariable var) {
        int idx = varList.size();
        varMap.put(var, idx);
        varList.add(var);
        return idx;
    }

    @Override
    public void clear() {
        varMap.clear();
        varList.clear();
    }

    @Override
    public int findVariable(LogicalVariable var) {
        Integer i = varMap.get(var);
        if (i == null) {
            return -1;
        }
        return i;
    }

    @Override
    public int getSize() {
        return varList.size();
    }

    @Override
    public LogicalVariable getVariable(int index) {
        return varList.get(index);
    }

    @Override
    public Iterator<LogicalVariable> iterator() {
        return varList.iterator();
    }

    @Override
    public String toString() {
        return varMap + " " + varList;
    }
}
