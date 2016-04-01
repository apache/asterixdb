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
package org.apache.asterix.lang.sqlpp.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.visitor.FreeVariableVisitor;

public class SqlppVariableUtil {

    private static String USER_VAR_PREFIX = "$";

    public static VarIdentifier toUserDefinedVariableName(VarIdentifier var) {
        String varName = var.getValue();
        return toUserDefinedVariableName(varName);
    }

    public static VarIdentifier toUserDefinedVariableName(String varName) {
        if (varName.startsWith(USER_VAR_PREFIX)) {
            return new VarIdentifier(varName.substring(1));
        }
        return new VarIdentifier(varName);
    }

    public static String toInternalVariableName(String varName) {
        return USER_VAR_PREFIX + varName;
    }

    public static VarIdentifier toInternalVariableIdentifier(String idName) {
        return new VarIdentifier(USER_VAR_PREFIX + idName);
    }

    public static Collection<VariableExpr> getFreeVariables(ILangExpression langExpr) throws AsterixException {
        Collection<VariableExpr> freeVars = new HashSet<>();
        FreeVariableVisitor visitor = new FreeVariableVisitor();
        langExpr.accept(visitor, freeVars);
        return freeVars;
    }

    public static Collection<VariableExpr> getBindingVariables(FromClause fromClause) {
        Set<VariableExpr> bindingVars = new HashSet<>();
        if (fromClause == null) {
            return bindingVars;
        }
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            bindingVars.addAll(getBindingVariables(fromTerm));
        }
        return bindingVars;
    }

    public static Collection<VariableExpr> getBindingVariables(FromTerm fromTerm) {
        Set<VariableExpr> bindingVars = new HashSet<>();
        if (fromTerm == null) {
            return bindingVars;
        }
        bindingVars.add(fromTerm.getLeftVariable());
        if (fromTerm.hasPositionalVariable()) {
            bindingVars.add(fromTerm.getPositionalVariable());
        }
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            bindingVars.add(correlateClause.getRightVariable());
            if (correlateClause.hasPositionalVariable()) {
                bindingVars.add(correlateClause.getPositionalVariable());
            }
        }
        return bindingVars;
    }

    public static Collection<VariableExpr> getBindingVariables(GroupbyClause gbyClause) {
        Set<VariableExpr> bindingVars = new HashSet<>();
        if (gbyClause == null) {
            return bindingVars;
        }
        for (GbyVariableExpressionPair gbyKey : gbyClause.getGbyPairList()) {
            VariableExpr var = gbyKey.getVar();
            if (var != null) {
                bindingVars.add(var);
            }
        }
        for (GbyVariableExpressionPair gbyKey : gbyClause.getDecorPairList()) {
            VariableExpr var = gbyKey.getVar();
            if (var != null) {
                bindingVars.add(var);
            }
        }
        bindingVars.addAll(gbyClause.getWithVarList());
        bindingVars.add(gbyClause.getGroupVar());
        return bindingVars;
    }

    public static Collection<VariableExpr> getBindingVariables(List<LetClause> letClauses) {
        Set<VariableExpr> bindingVars = new HashSet<>();
        if (letClauses == null || letClauses.isEmpty()) {
            return bindingVars;
        }
        for (LetClause letClause : letClauses) {
            bindingVars.add(letClause.getVarExpr());
        }
        return bindingVars;
    }

}
