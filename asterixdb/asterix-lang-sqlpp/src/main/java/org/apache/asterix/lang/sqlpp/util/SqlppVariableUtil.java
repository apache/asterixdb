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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
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

    private static final String USER_VAR_PREFIX = "$";

    private static final String EXTERNAL_VAR_PREFIX = "?";

    private SqlppVariableUtil() {
    }

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

    public static String variableNameToDisplayedFieldName(String varName) {
        if (varName.startsWith(USER_VAR_PREFIX)) {
            return varName.substring(1);
        } else {
            // We use prefix "$" for user-defined variables and "#" for system-generated variables.
            // However, in displayed query results, "$" is the prefix for
            // system-generated variables/alias. Therefore we need to replace the prefix
            // "#" with "$" if the system-generated alias needs to present in the final result.
            return USER_VAR_PREFIX + varName.substring(1);
        }
    }

    public static String toUserDefinedName(String varName) {
        if (varName.startsWith(USER_VAR_PREFIX)) {
            return varName.substring(1);
        }
        return varName;
    }

    public static String toInternalVariableName(String varName) {
        return USER_VAR_PREFIX + varName;
    }

    public static VarIdentifier toInternalVariableIdentifier(String idName) {
        return new VarIdentifier(toInternalVariableName(idName));
    }

    public static String toExternalVariableName(String varName) {
        return EXTERNAL_VAR_PREFIX + varName;
    }

    public static boolean isExternalVariableIdentifier(VarIdentifier varId) {
        return varId.getValue().startsWith(EXTERNAL_VAR_PREFIX);
    }

    public static boolean isExternalVariableReference(VariableExpr varExpr) {
        return isExternalVariableIdentifier(varExpr.getVar());
    }

    public static Collection<VariableExpr> getFreeVariables(ILangExpression langExpr) throws CompilationException {
        Collection<VariableExpr> freeVars = new HashSet<>();
        FreeVariableVisitor visitor = new FreeVariableVisitor();
        langExpr.accept(visitor, freeVars);
        return freeVars;
    }

    public static Collection<VariableExpr> getBindingVariables(FromClause fromClause) {
        if (fromClause == null) {
            return Collections.emptyList();
        }
        List<VariableExpr> bindingVars = new ArrayList<>();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            bindingVars.addAll(getBindingVariables(fromTerm));
        }
        return bindingVars;
    }

    public static Collection<VariableExpr> getBindingVariables(FromTerm fromTerm) {
        List<VariableExpr> bindingVars = new ArrayList<>();
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
        List<VariableExpr> bindingVars = new ArrayList<>();
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
        if (gbyClause.hasWithMap()) {
            bindingVars.addAll(gbyClause.getWithVarMap().values());
        }
        bindingVars.add(gbyClause.getGroupVar());
        return bindingVars;
    }

    public static Collection<VariableExpr> getBindingVariables(List<LetClause> letClauses) {
        List<VariableExpr> bindingVars = new ArrayList<>();
        if (letClauses == null || letClauses.isEmpty()) {
            return bindingVars;
        }
        for (LetClause letClause : letClauses) {
            bindingVars.add(letClause.getVarExpr());
        }
        return bindingVars;
    }

}
