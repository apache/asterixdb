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

import java.io.PrintWriter;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.visitor.FormatPrintVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SqlppFormatPrintVisitor extends FormatPrintVisitor implements ISqlppVisitor<Void, Integer> {

    public SqlppFormatPrintVisitor(PrintWriter out) {
        super(out);

        // Initialize symbols
        dataverseSymbol = " database ";
        datasetSymbol = " table ";
        assignSymbol = "=";
    }

    @Override
    public Void visit(FromClause fromClause, Integer step) throws CompilationException {
        out.print(skip(step) + "from ");
        int index = 0;
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            if (index > 0) {
                out.print(COMMA + "\n" + skip(step + 2));
            }
            fromTerm.accept(this, step + 2);
            ++index;
        }
        out.println();
        return null;
    }

    @Override
    public Void visit(FromTerm fromTerm, Integer step) throws CompilationException {
        fromTerm.getLeftExpression().accept(this, step + 2);
        out.print(" as ");
        fromTerm.getLeftVariable().accept(this, step + 2);
        if (fromTerm.hasPositionalVariable()) {
            out.print(" at ");
            fromTerm.getPositionalVariable().accept(this, step + 2);
        }
        if (fromTerm.hasCorrelateClauses()) {
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                correlateClause.accept(this, step);
            }
        }
        return null;
    }

    @Override
    public Void visit(JoinClause joinClause, Integer step) throws CompilationException {
        out.print(joinClause.getJoinType());
        joinClause.getRightExpression().accept(this, step + 2);
        out.print(" as ");
        joinClause.getRightVariable().accept(this, step + 2);
        if (joinClause.hasPositionalVariable()) {
            out.print(" at ");
            joinClause.getPositionalVariable().accept(this, step + 2);
        }
        joinClause.getConditionExpression().accept(this, step + 2);
        return null;
    }

    @Override
    public Void visit(NestClause nestClause, Integer step) throws CompilationException {
        out.print(nestClause.getJoinType());
        nestClause.getRightExpression().accept(this, step + 2);
        out.println(skip(step + 1) + " as ");
        nestClause.getRightVariable().accept(this, step + 2);
        if (nestClause.hasPositionalVariable()) {
            out.println(skip(step + 1) + " at ");
            nestClause.getPositionalVariable().accept(this, step + 2);
        }
        nestClause.getConditionExpression().accept(this, step + 2);
        return null;
    }

    @Override
    public Void visit(Projection projection, Integer step) throws CompilationException {
        if (projection.star()) {
            out.print(" * ");
            return null;
        }
        projection.getExpression().accept(this, step);
        if (projection.varStar()) {
            out.print(".* ");
        } else {
            String name = projection.getName();
            if (name != null) {
                out.print(" as " + name);
            }
        }
        return null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Integer step) throws CompilationException {
        selectBlock.getSelectClause().accept(this, step);
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, step);
        }
        if (selectBlock.hasLetWhereClauses()) {
            for (AbstractClause letWhereClause : selectBlock.getLetWhereList()) {
                letWhereClause.accept(this, step);
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, step);
            if (selectBlock.hasLetHavingClausesAfterGroupby()) {
                for (AbstractClause letHavingClause : selectBlock.getLetHavingListAfterGroupby()) {
                    letHavingClause.accept(this, step);
                }
            }
        }
        return null;
    }

    @Override
    public Void visit(SelectClause selectClause, Integer step) throws CompilationException {
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, step);
        }
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, step);
        }
        out.println();
        return null;
    }

    @Override
    public Void visit(SelectElement selectElement, Integer step) throws CompilationException {
        out.print("select element ");
        selectElement.getExpression().accept(this, step);
        return null;
    }

    @Override
    public Void visit(SelectRegular selectRegular, Integer step) throws CompilationException {
        out.print("select ");
        int index = 0;
        for (Projection projection : selectRegular.getProjections()) {
            if (index > 0) {
                out.print(COMMA);
            }
            projection.accept(this, step);
            ++index;
        }
        return null;
    }

    @Override
    public Void visit(SelectSetOperation selectSetOperation, Integer step) throws CompilationException {
        selectSetOperation.getLeftInput().accept(this, step);
        if (selectSetOperation.hasRightInputs()) {
            for (SetOperationRight right : selectSetOperation.getRightInputs()) {
                String all = right.isSetSemantics() ? " " : " all ";
                out.print(right.getSetOpType() + all);
                right.getSetOperationRightInput().accept(this, step);
            }
        }
        return null;
    }

    @Override
    public Void visit(SelectExpression selectStatement, Integer step) throws CompilationException {
        if (selectStatement.isSubquery()) {
            out.print("(");
        }
        int selectStep = selectStatement.isSubquery() ? step + 2 : step;
        if (selectStatement.hasLetClauses()) {
            for (LetClause letClause : selectStatement.getLetList()) {
                letClause.accept(this, selectStep);
            }
        }
        selectStatement.getSelectSetOperation().accept(this, selectStep);
        if (selectStatement.hasOrderby()) {
            selectStatement.getOrderbyClause().accept(this, selectStep);
        }
        if (selectStatement.hasLimit()) {
            selectStatement.getLimitClause().accept(this, selectStep);
        }
        if (selectStatement.isSubquery()) {
            out.print(skip(step) + " )");
        }
        return null;
    }

    @Override
    public Void visit(UnnestClause unnestClause, Integer step) throws CompilationException {
        out.print(unnestClause.getJoinType());
        unnestClause.getRightExpression().accept(this, step + 2);
        out.print(" as ");
        unnestClause.getRightVariable().accept(this, step + 2);
        if (unnestClause.hasPositionalVariable()) {
            out.print(" at ");
            unnestClause.getPositionalVariable().accept(this, step + 2);
        }
        return null;
    }

    @Override
    public Void visit(HavingClause havingClause, Integer step) throws CompilationException {
        out.print(skip(step) + " having ");
        havingClause.getFilterExpression().accept(this, step + 2);
        out.println();
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Integer step) throws CompilationException {
        if (gc.hasHashGroupByHint()) {
            out.println(skip(step) + "/* +hash */");
        }
        out.print(skip(step) + "group by ");
        printDelimitedGbyExpressions(gc.getGbyPairList(), step + 2);
        out.println();
        return null;
    }

    @Override
    public Void visit(InsertStatement insert, Integer step) throws CompilationException {
        out.print(skip(step) + "insert into " + datasetSymbol
                + generateFullName(insert.getDataverseName(), insert.getDatasetName()) + "\n");
        insert.getQuery().accept(this, step);
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(LetClause lc, Integer step) throws CompilationException {
        out.print(skip(step) + "with ");
        Expression bindingExpr = lc.getBindingExpr();
        bindingExpr.accept(this, step + 2);
        out.print(" as ");
        lc.getVarExpr().accept(this, step + 2);
        out.println();
        return null;
    }

    @Override
    protected void printDelimitedGbyExpressions(List<GbyVariableExpressionPair> gbyList, int step)
            throws CompilationException {
        int gbySize = gbyList.size();
        int gbyIndex = 0;
        for (GbyVariableExpressionPair pair : gbyList) {
            pair.getExpr().accept(this, step);
            if (pair.getVar() != null) {
                out.print(" as ");
                pair.getVar().accept(this, step);
            }
            if (++gbyIndex < gbySize) {
                out.print(COMMA);
            }
        }
    }

    @Override
    public Void visit(CaseExpression caseExpr, Integer step) throws CompilationException {
        out.print(skip(step) + "case ");
        caseExpr.getConditionExpr().accept(this, step + 2);
        out.println();
        List<Expression> whenExprs = caseExpr.getWhenExprs();
        List<Expression> thenExprs = caseExpr.getThenExprs();
        for (int index = 0; index < whenExprs.size(); ++index) {
            out.print(skip(step) + "when ");
            whenExprs.get(index).accept(this, step + 2);
            out.print(" then ");
            thenExprs.get(index).accept(this, step + 2);
            out.println();
        }
        out.print(skip(step) + "else ");
        caseExpr.getElseExpr().accept(this, step + 2);
        out.println();
        out.println(skip(step) + "end");
        return null;
    }

    @Override
    public Void visit(VariableExpr v, Integer step) {
        out.print(SqlppVariableUtil.toUserDefinedName(v.getVar().getValue()));
        return null;
    }

    @Override
    public Void visit(WindowExpression windowExpr, Integer step) throws CompilationException {
        out.print(skip(step) + "window ");
        out.print(generateFullName(windowExpr.getFunctionSignature().getNamespace(),
                windowExpr.getFunctionSignature().getName()) + "(");
        printDelimitedExpressions(windowExpr.getExprList(), COMMA, step);
        out.print(")");
        out.print(skip(step) + " over ");
        if (windowExpr.hasWindowVar()) {
            windowExpr.getWindowVar().accept(this, step + 2);
            out.print(skip(step) + "as ");
        }
        out.print("(");
        if (windowExpr.hasPartitionList()) {
            List<Expression> partitionList = windowExpr.getPartitionList();
            for (int i = 0, ln = partitionList.size(); i < ln; i++) {
                if (i > 0) {
                    out.print(COMMA);
                }
                Expression partExpr = partitionList.get(i);
                partExpr.accept(this, step + 2);
            }
        }
        if (windowExpr.hasOrderByList()) {
            out.print(skip(step) + " order by ");
            printDelimitedObyExpressions(windowExpr.getOrderbyList(), windowExpr.getOrderbyModifierList(), step + 2);
        }
        if (windowExpr.hasFrameDefinition()) {
            out.println(skip(step) + windowExpr.getFrameMode());
            if (windowExpr.hasFrameStartExpr()) {
                windowExpr.getFrameStartExpr().accept(this, step + 2);
            }
            out.println(skip(step) + windowExpr.getFrameStartKind());
            if (windowExpr.hasFrameEndExpr()) {
                windowExpr.getFrameEndExpr().accept(this, step + 2);
            }
            out.println(skip(step) + windowExpr.getFrameEndKind());
            out.println(skip(step) + "exclude " + windowExpr.getFrameExclusionKind());
        }
        out.println(skip(step) + ")");
        return null;
    }
}
