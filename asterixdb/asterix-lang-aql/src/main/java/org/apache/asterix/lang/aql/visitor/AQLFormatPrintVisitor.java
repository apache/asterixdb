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
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.FormatPrintVisitor;

public class AQLFormatPrintVisitor extends FormatPrintVisitor implements IAQLVisitor<Void, Integer> {

    public AQLFormatPrintVisitor(PrintWriter out) {
        super(out);
    }

    @Override
    public Void visit(FLWOGRExpression flwor, Integer step) throws CompilationException {
        for (Clause cl : flwor.getClauseList()) {
            cl.accept(this, step);
        }
        out.print(skip(step) + "return ");
        flwor.getReturnExpr().accept(this, step + 2);
        return null;
    }

    @Override
    public Void visit(ForClause fc, Integer step) throws CompilationException {
        out.print("for ");
        fc.getVarExpr().accept(this, step + 2);
        if (fc.hasPosVar()) {
            out.print(" at ");
            fc.getPosVarExpr().accept(this, step + 2);
        }
        out.print(" in ");
        fc.getInExpr().accept(this, step + 2);
        out.println();
        return null;
    }

    @Override
    public Void visit(UnionExpr u, Integer step) throws CompilationException {
        printDelimitedExpressions(u.getExprs(), "\n" + skip(step) + "union\n", step);
        return null;
    }

    @Override
    public Void visit(DistinctClause dc, Integer step) throws CompilationException {
        out.print(skip(step) + "distinct by ");
        printDelimitedExpressions(dc.getDistinctByExpr(), COMMA, step + 2);
        out.println();
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Integer step) throws CompilationException {
        if (gc.hasHashGroupByHint()) {
            out.println(skip(step) + "/* +hash */");
        }
        out.print(skip(step) + "group by ");
        List<List<GbyVariableExpressionPair>> gbyList = gc.getGbyPairList();
        if (gbyList.size() == 1) {
            printDelimitedGbyExpressions(gbyList.get(0), step + 2);
        } else {
            // AQL does not support grouping sets
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, gc.getSourceLocation(), "");
        }
        if (gc.hasDecorList()) {
            out.print(" decor ");
            printDelimitedGbyExpressions(gc.getDecorPairList(), step + 2);
        }
        if (gc.hasWithMap()) {
            out.print(" with ");
            Map<Expression, VariableExpr> withVarMap = gc.getWithVarMap();
            int index = 0;
            int size = withVarMap.size();
            for (Map.Entry<Expression, VariableExpr> entry : withVarMap.entrySet()) {
                Expression key = entry.getKey();
                VariableExpr value = entry.getValue();
                key.accept(this, step + 2);
                if (!key.equals(value)) {
                    out.print(" as ");
                    value.accept(this, step + 2);
                }
                if (++index < size) {
                    out.print(COMMA);
                }
            }
        }
        out.println();
        return null;
    }
}
