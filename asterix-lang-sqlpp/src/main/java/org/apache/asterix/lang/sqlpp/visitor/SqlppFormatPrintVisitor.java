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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
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
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SqlppFormatPrintVisitor extends FormatPrintVisitor implements ISqlppVisitor<Void, Integer> {

    private final PrintWriter out;

    public SqlppFormatPrintVisitor() {
        this(new PrintWriter(System.out));
    }

    public SqlppFormatPrintVisitor(PrintWriter out) {
        super(out);
        this.out = out;

        // Initialize symbols
        dataverseSymbol = " database ";
        datasetSymbol = " table ";
        assignSymbol = "=";
    }

    @Override
    public Void visit(FromClause fromClause, Integer step) throws AsterixException {
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
    public Void visit(FromTerm fromTerm, Integer step) throws AsterixException {
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
    public Void visit(JoinClause joinClause, Integer step) throws AsterixException {
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
    public Void visit(NestClause nestClause, Integer step) throws AsterixException {
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
    public Void visit(Projection projection, Integer step) throws AsterixException {
        projection.getExpression().accept(this, step);
        out.print(" as " + projection.getName());
        return null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Integer step) throws AsterixException {
        selectBlock.getSelectClause().accept(this, step);
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, step);
        }
        if (selectBlock.hasLetClauses()) {
            for (LetClause letClause : selectBlock.getLetList()) {
                letClause.accept(this, step);
            }
        }
        if (selectBlock.hasWhereClause()) {
            selectBlock.getWhereClause().accept(this, step);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, step);
            if (selectBlock.hasLetClausesAfterGroupby()) {
                for (LetClause letClause : selectBlock.getLetListAfterGroupby()) {
                    letClause.accept(this, step);
                }
            }
        }
        if (selectBlock.hasHavingClause()) {
            selectBlock.getHavingClause().accept(this, step);
        }
        return null;
    }

    @Override
    public Void visit(SelectClause selectClause, Integer step) throws AsterixException {
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
    public Void visit(SelectElement selectElement, Integer step) throws AsterixException {
        out.print("select element ");
        selectElement.getExpression().accept(this, step);
        return null;
    }

    @Override
    public Void visit(SelectRegular selectRegular, Integer step) throws AsterixException {
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
    public Void visit(SelectSetOperation selectSetOperation, Integer step) throws AsterixException {
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
    public Void visit(SelectExpression selectStatement, Integer step) throws AsterixException {
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
    public Void visit(UnnestClause unnestClause, Integer step) throws AsterixException {
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
    public Void visit(HavingClause havingClause, Integer step) throws AsterixException {
        out.print(skip(step) + " having ");
        havingClause.getFilterExpression().accept(this, step + 2);
        out.println();
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Integer step) throws AsterixException {
        if (gc.hasHashGroupByHint()) {
            out.println(skip(step) + "/* +hash */");
        }
        out.print(skip(step) + "group by ");
        printDelimitedGbyExpressions(gc.getGbyPairList(), step + 2);
        out.println();
        return null;
    }

    @Override
    public Void visit(InsertStatement insert, Integer step) throws AsterixException {
        out.print(skip(step) + "insert into " + datasetSymbol
                + generateFullName(insert.getDataverseName(), insert.getDatasetName()) + "\n");
        insert.getQuery().accept(this, step);
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(LetClause lc, Integer step) throws AsterixException {
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
            throws AsterixException {
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

}
