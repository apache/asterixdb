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
package org.apache.asterix.lang.common.rewrites;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class VariableSubstitutionEnvironment {

    private Map<String, Expression> oldVarToNewExpressionMap = new HashMap<String, Expression>();

    public VariableSubstitutionEnvironment() {
        // Default constructor.
    }

    public VariableSubstitutionEnvironment(Map<VariableExpr, Expression> varExprMap) {
        varExprMap.forEach((key, value) -> oldVarToNewExpressionMap.put(key.getVar().getValue(), value));
    }

    public VariableSubstitutionEnvironment(VariableSubstitutionEnvironment env) {
        oldVarToNewExpressionMap.putAll(env.oldVarToNewExpressionMap);
    }

    public void addMappings(Map<String, Expression> varExprMap) {
        oldVarToNewExpressionMap.putAll(varExprMap);
    }

    public Expression findSubstitution(VariableExpr oldVar) {
        return oldVarToNewExpressionMap.get(oldVar.getVar().getValue());
    }

    public boolean constainsOldVar(VariableExpr oldVar) {
        return oldVarToNewExpressionMap.containsKey(oldVar.getVar().getValue());
    }

    public void addSubstituion(VariableExpr oldVar, Expression newExpr) {
        oldVarToNewExpressionMap.put(oldVar.getVar().getValue(), newExpr);
    }

    public void removeSubstitution(VariableExpr oldVar) {
        if (oldVar == null) {
            return;
        }
        oldVarToNewExpressionMap.remove(oldVar.getVar().getValue());
    }

    @Override
    public String toString() {
        return oldVarToNewExpressionMap.toString();
    }
}
