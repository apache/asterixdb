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
package org.apache.asterix.translator;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

public final class TranslationContext {

    private Counter varCounter;

    /** The stack is the used to manage the scope of variables for group-by rebindings. */
    private Stack<Map<Integer, LogicalVariable>> stack = new Stack<Map<Integer, LogicalVariable>>();
    private Map<Integer, LogicalVariable> currentVarMap = new HashMap<Integer, LogicalVariable>();
    private boolean topFlwor = true;

    public TranslationContext(Counter varCounter) {
        this.varCounter = varCounter;
    }

    public int getVarCounter() {
        return varCounter.get();
    }

    public boolean isTopFlwor() {
        return topFlwor;
    }

    public void setTopFlwor(boolean b) {
        topFlwor = b;
    }

    public LogicalVariable getVar(Integer varId) {
        return currentVarMap.get(varId);
    }

    public LogicalVariable getVar(VariableExpr v) {
        return currentVarMap.get(v.getVar().getId());
    }

    public LogicalVariable newVar(VariableExpr v) {
        Integer i = v.getVar().getId();
        if (i > varCounter.get()) {
            varCounter.set(i);
        }
        LogicalVariable var = new LogicalVariable(i);
        currentVarMap.put(i, var);
        return var;
    }

    public void setVar(VariableExpr v, LogicalVariable var) {
        currentVarMap.put(v.getVar().getId(), var);
    }

    public LogicalVariable newVar() {
        varCounter.inc();
        LogicalVariable var = new LogicalVariable(varCounter.get());
        currentVarMap.put(varCounter.get(), var);
        return var;
    }

    /**
     * Within a subplan, an unbounded variable can be rebound in
     * the group-by operator. But the rebinding only exists
     * in the subplan.
     * This method marks that the translation enters a subplan.
     */
    public void enterSubplan() {
        Map<Integer, LogicalVariable> varMap = new HashMap<Integer, LogicalVariable>();
        varMap.putAll(currentVarMap);
        stack.push(currentVarMap);
        currentVarMap = varMap;
    }

    /***
     * This method marks that the translation exits a subplan.
     */
    public void existSubplan() {
        if (!stack.isEmpty()) {
            currentVarMap = stack.pop();
        }
    }

    /**
     * @return the variables produced by the top operator in a subplan.
     */
    public LogicalVariable newSubplanOutputVar() {
        LogicalVariable newVar = newVar();
        if (!stack.isEmpty()) {
            Map<Integer, LogicalVariable> varMap = stack.peek();
            varMap.put(varCounter.get(), newVar);
        }
        return newVar;
    }
}
