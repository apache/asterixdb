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

import java.io.PrintWriter;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.visitor.QueryPrintVisitor;

class AQLAstPrintVisitor extends QueryPrintVisitor implements IAQLVisitor<Void, Integer> {

    public AQLAstPrintVisitor(PrintWriter out) {
        super(out);
    }

    @Override
    public Void visit(FLWOGRExpression flwor, Integer step) throws CompilationException {
        out.println(skip(step) + "FLWOGR [");
        for (Clause cl : flwor.getClauseList()) {
            cl.accept(this, step + 1);
        }
        out.println(skip(step + 1) + "Return");
        flwor.getReturnExpr().accept(this, step + 2);
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(ForClause fc, Integer step) throws CompilationException {
        out.print(skip(step) + "For ");
        fc.getVarExpr().accept(this, 0);
        out.println(skip(step + 1) + "In ");
        fc.getInExpr().accept(this, step + 1);
        return null;
    }

    @Override
    public Void visit(UnionExpr u, Integer step) throws CompilationException {
        out.println(skip(step) + "Union [");
        for (Expression expr : u.getExprs()) {
            expr.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(DistinctClause dc, Integer step) throws CompilationException {
        out.print(skip(step) + "Distinct ");
        for (Expression expr : dc.getDistinctByExpr()) {
            expr.accept(this, step + 1);
        }
        return null;
    }

    @Override
    public Void visit(ListSliceExpression expression, Integer step) throws CompilationException {
        // This functionality is not supported for AQL
        return null;
    }

}
