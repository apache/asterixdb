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
package org.apache.asterix.lang.sqlpp.visitor;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppAstVisitor;

public class BindingVariableVisitor extends AbstractSqlppAstVisitor<Void, Collection<VariableExpr>> {
    @Override
    public Void visit(FromClause fromClause, Collection<VariableExpr> bindingVars) throws CompilationException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, bindingVars);
        }
        return null;
    }

    @Override
    public Void visit(FromTerm fromTerm, Collection<VariableExpr> bindingVars) throws CompilationException {
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
        return null;
    }

    @Override
    public Void visit(GroupbyClause groupbyClause, Collection<VariableExpr> bindingVars) throws CompilationException {
        Set<VariableExpr> gbyKeyVars = new HashSet<>();
        for (List<GbyVariableExpressionPair> gbyPairList : groupbyClause.getGbyPairList()) {
            for (GbyVariableExpressionPair gbyKey : gbyPairList) {
                VariableExpr var = gbyKey.getVar();
                if (var != null && gbyKeyVars.add(var)) {
                    bindingVars.add(var);
                }
            }
        }
        if (groupbyClause.hasDecorList()) {
            for (GbyVariableExpressionPair gbyKey : groupbyClause.getDecorPairList()) {
                VariableExpr var = gbyKey.getVar();
                if (var != null) {
                    bindingVars.add(var);
                }
            }
        }
        if (groupbyClause.hasWithMap()) {
            bindingVars.addAll(groupbyClause.getWithVarMap().values());
        }
        bindingVars.add(groupbyClause.getGroupVar());
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Collection<VariableExpr> bindingVars) throws CompilationException {
        List<QuantifiedPair> quantifiedList = qe.getQuantifiedList();
        for (QuantifiedPair qp : quantifiedList) {
            bindingVars.add(qp.getVarExpr());
        }
        return null;
    }

    @Override
    public Void visit(IVisitorExtension ve, Collection<VariableExpr> arg) throws CompilationException {
        return ve.bindingVariableDispatch(this, arg);
    }
}
