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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;

public class OperatorSchemaImpl implements IOperatorSchema {
    private final Map<LogicalVariable, Integer> varMap;

    private final List<LogicalVariable> varList;

    public OperatorSchemaImpl() {
        varMap = new HashMap<LogicalVariable, Integer>();
        varList = new ArrayList<LogicalVariable>();
    }

    @Override
    public void addAllVariables(IOperatorSchema source) {
        for (LogicalVariable v : source) {
            varMap.put(v, varList.size());
            varList.add(v);
        }
    }

    @Override
    public void addAllNewVariables(IOperatorSchema source) {
        for (LogicalVariable v : source) {
            if (varMap.get(v) == null) {
                varMap.put(v, varList.size());
                varList.add(v);
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
        return varMap.toString();
    }
}
