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
package org.apache.asterix.lang.aql.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.visitor.AbstractInlineUdfsVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class AQLInlineUdfsVisitor extends AbstractInlineUdfsVisitor
        implements IAQLVisitor<Boolean, List<FunctionDecl>> {

    public AQLInlineUdfsVisitor(LangRewritingContext context, IRewriterFactory rewriterFactory,
            List<FunctionDecl> declaredFunctions, MetadataProvider metadataProvider) {
        super(context, rewriterFactory, declaredFunctions, metadataProvider,
                new AQLCloneAndSubstituteVariablesVisitor(context));
    }

    @Override
    public Boolean visit(FLWOGRExpression flwor, List<FunctionDecl> arg) throws CompilationException {
        boolean changed = false;
        for (Clause c : flwor.getClauseList()) {
            if (c.accept(this, arg)) {
                changed = true;
            }
        }
        Pair<Boolean, Expression> p = inlineUdfsInExpr(flwor.getReturnExpr(), arg);
        flwor.setReturnExpr(p.second);
        return changed || p.first;
    }

    @Override
    public Boolean visit(ForClause fc, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fc.getInExpr(), arg);
        fc.setInExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(UnionExpr u, List<FunctionDecl> fds) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(u.getExprs(), fds);
        u.setExprs(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(DistinctClause dc, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(dc.getDistinctByExpr(), arg);
        dc.setDistinctByExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(ListSliceExpression expression, List<FunctionDecl> arg) throws CompilationException {
        // This functionality is not supported for AQL
        return false;
    }

    @Override
    protected Expression generateQueryExpression(List<LetClause> letClauses, Expression returnExpr) {
        List<Clause> letList = new ArrayList<Clause>();
        letList.addAll(letClauses);
        return new FLWOGRExpression(letList, returnExpr);
    }
}
