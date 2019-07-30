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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
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
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class DeepCopyVisitor extends AbstractSqlppQueryExpressionVisitor<ILangExpression, Void> {

    @Override
    public FromClause visit(FromClause fromClause, Void arg) throws CompilationException {
        List<FromTerm> fromTerms = new ArrayList<>();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerms.add((FromTerm) fromTerm.accept(this, arg));
        }
        FromClause copy = new FromClause(fromTerms);
        copy.setSourceLocation(fromClause.getSourceLocation());
        return copy;
    }

    @Override
    public FromTerm visit(FromTerm fromTerm, Void arg) throws CompilationException {
        // Visit the left expression of a from term.
        Expression fromExpr = (Expression) fromTerm.getLeftExpression().accept(this, arg);
        VariableExpr fromVar = (VariableExpr) fromTerm.getLeftVariable().accept(this, arg);
        VariableExpr positionVar = fromTerm.getPositionalVariable() == null ? null
                : (VariableExpr) fromTerm.getPositionalVariable().accept(this, arg);

        // Visits join/unnest/nest clauses.
        List<AbstractBinaryCorrelateClause> correlateClauses = new ArrayList<>();
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClauses.add((AbstractBinaryCorrelateClause) correlateClause.accept(this, arg));
        }
        FromTerm copy = new FromTerm(fromExpr, fromVar, positionVar, correlateClauses);
        copy.setSourceLocation(fromTerm.getSourceLocation());
        return copy;
    }

    @Override
    public JoinClause visit(JoinClause joinClause, Void arg) throws CompilationException {
        Expression rightExpression = (Expression) joinClause.getRightExpression().accept(this, arg);
        VariableExpr rightVar = (VariableExpr) joinClause.getRightVariable().accept(this, arg);
        VariableExpr rightPositionVar = joinClause.getPositionalVariable() == null ? null
                : (VariableExpr) joinClause.getPositionalVariable().accept(this, arg);
        Expression conditionExpresion = (Expression) joinClause.getConditionExpression().accept(this, arg);
        JoinClause copy = new JoinClause(joinClause.getJoinType(), rightExpression, rightVar, rightPositionVar,
                conditionExpresion);
        copy.setSourceLocation(joinClause.getSourceLocation());
        return copy;
    }

    @Override
    public NestClause visit(NestClause nestClause, Void arg) throws CompilationException {
        Expression rightExpression = (Expression) nestClause.getRightExpression().accept(this, arg);
        VariableExpr rightVar = (VariableExpr) nestClause.getRightVariable().accept(this, arg);
        VariableExpr rightPositionVar = nestClause.getPositionalVariable() == null ? null
                : (VariableExpr) nestClause.getPositionalVariable().accept(this, arg);
        Expression conditionExpresion = (Expression) nestClause.getConditionExpression().accept(this, arg);
        NestClause copy = new NestClause(nestClause.getJoinType(), rightExpression, rightVar, rightPositionVar,
                conditionExpresion);
        copy.setSourceLocation(nestClause.getSourceLocation());
        return copy;
    }

    @Override
    public UnnestClause visit(UnnestClause unnestClause, Void arg) throws CompilationException {
        Expression rightExpression = (Expression) unnestClause.getRightExpression().accept(this, arg);
        VariableExpr rightVar = (VariableExpr) unnestClause.getRightVariable().accept(this, arg);
        VariableExpr rightPositionVar = unnestClause.getPositionalVariable() == null ? null
                : (VariableExpr) unnestClause.getPositionalVariable().accept(this, arg);
        UnnestClause copy = new UnnestClause(unnestClause.getJoinType(), rightExpression, rightVar, rightPositionVar);
        copy.setSourceLocation(unnestClause.getSourceLocation());
        return copy;
    }

    @Override
    public Projection visit(Projection projection, Void arg) throws CompilationException {
        Projection copy =
                new Projection(projection.star() ? null : (Expression) projection.getExpression().accept(this, arg),
                        projection.getName(), projection.star(), projection.varStar());
        copy.setSourceLocation(projection.getSourceLocation());
        return copy;
    }

    @Override
    public SelectBlock visit(SelectBlock selectBlock, Void arg) throws CompilationException {
        FromClause fromClause = null;
        List<AbstractClause> letWhereClauses = new ArrayList<>();
        GroupbyClause gbyClause = null;
        List<AbstractClause> gbyLetHavingClauses = new ArrayList<>();
        SelectClause selectClause;
        // Traverses the select block in the order of "from", "let/where"s, "group by", "let/having"s, and "select".
        if (selectBlock.hasFromClause()) {
            fromClause = (FromClause) selectBlock.getFromClause().accept(this, arg);
        }
        if (selectBlock.hasLetWhereClauses()) {
            List<AbstractClause> letWhereList = selectBlock.getLetWhereList();
            for (AbstractClause letWhereClause : letWhereList) {
                letWhereClauses.add((AbstractClause) letWhereClause.accept(this, arg));
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            gbyClause = (GroupbyClause) selectBlock.getGroupbyClause().accept(this, arg);
        }
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            List<AbstractClause> letHavingListAfterGby = selectBlock.getLetHavingListAfterGroupby();
            for (AbstractClause letHavingClauseAfterGby : letHavingListAfterGby) {
                gbyLetHavingClauses.add((AbstractClause) letHavingClauseAfterGby.accept(this, arg));
            }
        }
        selectClause = (SelectClause) selectBlock.getSelectClause().accept(this, arg);
        SelectBlock copy = new SelectBlock(selectClause, fromClause, letWhereClauses, gbyClause, gbyLetHavingClauses);
        copy.setSourceLocation(selectBlock.getSourceLocation());
        return copy;
    }

    @Override
    public SelectClause visit(SelectClause selectClause, Void arg) throws CompilationException {
        SelectElement selectElement = null;
        SelectRegular selectRegular = null;
        if (selectClause.selectElement()) {
            selectElement = (SelectElement) selectClause.getSelectElement().accept(this, arg);
        }
        if (selectClause.selectRegular()) {
            selectRegular = (SelectRegular) selectClause.getSelectRegular().accept(this, arg);
        }
        SelectClause copy = new SelectClause(selectElement, selectRegular, selectClause.distinct());
        copy.setSourceLocation(selectClause.getSourceLocation());
        return copy;
    }

    @Override
    public SelectElement visit(SelectElement selectElement, Void arg) throws CompilationException {
        SelectElement copy = new SelectElement((Expression) selectElement.getExpression().accept(this, arg));
        copy.setSourceLocation(selectElement.getSourceLocation());
        return copy;
    }

    @Override
    public SelectRegular visit(SelectRegular selectRegular, Void arg) throws CompilationException {
        List<Projection> projections = new ArrayList<>();
        for (Projection projection : selectRegular.getProjections()) {
            projections.add((Projection) projection.accept(this, arg));
        }
        SelectRegular copy = new SelectRegular(projections);
        copy.setSourceLocation(selectRegular.getSourceLocation());
        return copy;
    }

    @Override
    public SelectSetOperation visit(SelectSetOperation selectSetOperation, Void arg) throws CompilationException {
        SetOperationInput leftInput = selectSetOperation.getLeftInput();
        SetOperationInput newLeftInput;
        if (leftInput.selectBlock()) {
            newLeftInput = new SetOperationInput((SelectBlock) leftInput.accept(this, arg), null);
        } else {
            newLeftInput = new SetOperationInput(null, (SelectExpression) leftInput.accept(this, arg));
        }
        List<SetOperationRight> rightInputs = new ArrayList<>();
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            SetOperationInput newRightInput;
            SetOperationInput setOpRightInput = right.getSetOperationRightInput();
            if (setOpRightInput.selectBlock()) {
                newRightInput = new SetOperationInput((SelectBlock) setOpRightInput.accept(this, arg), null);
            } else {
                newRightInput = new SetOperationInput(null, (SelectExpression) setOpRightInput.accept(this, arg));
            }
            rightInputs.add(new SetOperationRight(right.getSetOpType(), right.isSetSemantics(), newRightInput));
        }
        SelectSetOperation copy = new SelectSetOperation(newLeftInput, rightInputs);
        copy.setSourceLocation(selectSetOperation.getSourceLocation());
        return copy;
    }

    @Override
    public HavingClause visit(HavingClause havingClause, Void arg) throws CompilationException {
        HavingClause copy = new HavingClause((Expression) havingClause.getFilterExpression().accept(this, arg));
        copy.setSourceLocation(havingClause.getSourceLocation());
        return copy;
    }

    @Override
    public Query visit(Query q, Void arg) throws CompilationException {
        Query copy =
                new Query(q.isExplain(), q.isTopLevel(), (Expression) q.getBody().accept(this, arg), q.getVarCounter());
        copy.setSourceLocation(q.getSourceLocation());
        return copy;
    }

    @Override
    public FunctionDecl visit(FunctionDecl fd, Void arg) throws CompilationException {
        FunctionDecl copy =
                new FunctionDecl(fd.getSignature(), fd.getParamList(), (Expression) fd.getFuncBody().accept(this, arg));
        copy.setSourceLocation(fd.getSourceLocation());
        return copy;
    }

    @Override
    public WhereClause visit(WhereClause whereClause, Void arg) throws CompilationException {
        WhereClause copy = new WhereClause((Expression) whereClause.getWhereExpr().accept(this, arg));
        copy.setSourceLocation(whereClause.getSourceLocation());
        return copy;
    }

    @Override
    public OrderbyClause visit(OrderbyClause oc, Void arg) throws CompilationException {
        List<Expression> newOrderbyList = new ArrayList<>();
        for (Expression orderExpr : oc.getOrderbyList()) {
            newOrderbyList.add((Expression) orderExpr.accept(this, arg));
        }
        OrderbyClause copy = new OrderbyClause(newOrderbyList, new ArrayList<>(oc.getModifierList()));
        copy.setSourceLocation(oc.getSourceLocation());
        return copy;
    }

    @Override
    public GroupbyClause visit(GroupbyClause gc, Void arg) throws CompilationException {
        List<GbyVariableExpressionPair> gbyPairList = new ArrayList<>();
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            VariableExpr var = gbyVarExpr.getVar();
            gbyPairList.add(new GbyVariableExpressionPair(var == null ? null : (VariableExpr) var.accept(this, arg),
                    (Expression) gbyVarExpr.getExpr().accept(this, arg)));
        }
        List<GbyVariableExpressionPair> decorPairList = new ArrayList<>();
        if (gc.hasDecorList()) {
            for (GbyVariableExpressionPair gbyVarExpr : gc.getDecorPairList()) {
                VariableExpr var = gbyVarExpr.getVar();
                decorPairList
                        .add(new GbyVariableExpressionPair(var == null ? null : (VariableExpr) var.accept(this, arg),
                                (Expression) gbyVarExpr.getExpr().accept(this, arg)));
            }
        }
        Map<Expression, VariableExpr> withVarMap = new HashMap<>();
        if (gc.hasWithMap()) {
            for (Entry<Expression, VariableExpr> entry : gc.getWithVarMap().entrySet()) {
                withVarMap.put((Expression) entry.getKey().accept(this, arg),
                        (VariableExpr) entry.getValue().accept(this, arg));
            }
        }
        VariableExpr groupVarExpr = null;
        if (gc.hasGroupVar()) {
            groupVarExpr = (VariableExpr) gc.getGroupVar().accept(this, arg);
        }
        List<Pair<Expression, Identifier>> groupFieldList = copyFieldList(gc.getGroupFieldList(), arg);
        GroupbyClause copy = new GroupbyClause(gbyPairList, decorPairList, withVarMap, groupVarExpr, groupFieldList,
                gc.hasHashGroupByHint(), gc.isGroupAll());
        copy.setSourceLocation(gc.getSourceLocation());
        return copy;
    }

    @Override
    public LimitClause visit(LimitClause limitClause, Void arg) throws CompilationException {
        Expression limitExpr = (Expression) limitClause.getLimitExpr().accept(this, arg);
        Expression offsetExpr = limitClause.hasOffset() ? (Expression) limitClause.getOffset().accept(this, arg) : null;
        LimitClause copy = new LimitClause(limitExpr, offsetExpr);
        copy.setSourceLocation(limitClause.getSourceLocation());
        return copy;
    }

    @Override
    public LetClause visit(LetClause letClause, Void arg) throws CompilationException {
        LetClause copy = new LetClause((VariableExpr) letClause.getVarExpr().accept(this, arg),
                (Expression) letClause.getBindingExpr().accept(this, arg));
        copy.setSourceLocation(letClause.getSourceLocation());
        return copy;
    }

    @Override
    public SelectExpression visit(SelectExpression selectExpression, Void arg) throws CompilationException {
        List<LetClause> lets = new ArrayList<>();
        SelectSetOperation select;
        OrderbyClause orderby = null;
        LimitClause limit = null;

        // visit let list
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                lets.add((LetClause) letClause.accept(this, arg));
            }
        }

        // visit the main select.
        select = (SelectSetOperation) selectExpression.getSelectSetOperation().accept(this, arg);

        // visit order by
        if (selectExpression.hasOrderby()) {
            orderby = (OrderbyClause) selectExpression.getOrderbyClause().accept(this, arg);
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            limit = (LimitClause) selectExpression.getLimitClause().accept(this, arg);
        }

        SelectExpression copy = new SelectExpression(lets, select, orderby, limit, selectExpression.isSubquery());
        copy.setSourceLocation(select.getSourceLocation());
        copy.addHints(selectExpression.getHints());

        return copy;
    }

    @Override
    public LiteralExpr visit(LiteralExpr l, Void arg) throws CompilationException {
        return l;
    }

    @Override
    public ListConstructor visit(ListConstructor lc, Void arg) throws CompilationException {
        ListConstructor copy = new ListConstructor(lc.getType(), copyExprList(lc.getExprList(), arg));
        copy.setSourceLocation(lc.getSourceLocation());
        copy.addHints(lc.getHints());
        return copy;
    }

    @Override
    public RecordConstructor visit(RecordConstructor rc, Void arg) throws CompilationException {
        List<FieldBinding> bindings = new ArrayList<>();
        for (FieldBinding binding : rc.getFbList()) {
            FieldBinding fb = new FieldBinding((Expression) binding.getLeftExpr().accept(this, arg),
                    (Expression) binding.getRightExpr().accept(this, arg));
            bindings.add(fb);
        }
        RecordConstructor copy = new RecordConstructor(bindings);
        copy.setSourceLocation(rc.getSourceLocation());
        copy.addHints(rc.getHints());
        return copy;
    }

    @Override
    public OperatorExpr visit(OperatorExpr operatorExpr, Void arg) throws CompilationException {
        OperatorExpr copy = new OperatorExpr(copyExprList(operatorExpr.getExprList(), arg),
                operatorExpr.getExprBroadcastIdx(), operatorExpr.getOpList(), operatorExpr.isCurrentop());
        copy.setSourceLocation(operatorExpr.getSourceLocation());
        copy.addHints(operatorExpr.getHints());
        return copy;
    }

    @Override
    public IfExpr visit(IfExpr ifExpr, Void arg) throws CompilationException {
        Expression conditionExpr = (Expression) ifExpr.getCondExpr().accept(this, arg);
        Expression thenExpr = (Expression) ifExpr.getThenExpr().accept(this, arg);
        Expression elseExpr = (Expression) ifExpr.getElseExpr().accept(this, arg);
        IfExpr copy = new IfExpr(conditionExpr, thenExpr, elseExpr);
        copy.setSourceLocation(ifExpr.getSourceLocation());
        copy.addHints(ifExpr.getHints());
        return copy;
    }

    @Override
    public QuantifiedExpression visit(QuantifiedExpression qe, Void arg) throws CompilationException {
        List<QuantifiedPair> quantifiedPairs = new ArrayList<>();
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            Expression expr = (Expression) pair.getExpr().accept(this, arg);
            VariableExpr var = (VariableExpr) pair.getVarExpr().accept(this, arg);
            quantifiedPairs.add(new QuantifiedPair(var, expr));
        }
        Expression condition = (Expression) qe.getSatisfiesExpr().accept(this, arg);
        QuantifiedExpression copy = new QuantifiedExpression(qe.getQuantifier(), quantifiedPairs, condition);
        copy.setSourceLocation(qe.getSourceLocation());
        copy.addHints(qe.getHints());
        return copy;
    }

    @Override
    public CallExpr visit(CallExpr callExpr, Void arg) throws CompilationException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : callExpr.getExprList()) {
            newExprList.add((Expression) expr.accept(this, arg));
        }
        CallExpr copy = new CallExpr(callExpr.getFunctionSignature(), newExprList);
        copy.setSourceLocation(callExpr.getSourceLocation());
        copy.addHints(callExpr.getHints());
        return copy;
    }

    @Override
    public VariableExpr visit(VariableExpr varExpr, Void arg) throws CompilationException {
        VariableExpr clonedVar = new VariableExpr(new VarIdentifier(varExpr.getVar()));
        clonedVar.setSourceLocation(varExpr.getSourceLocation());
        clonedVar.setIsNewVar(varExpr.getIsNewVar());
        clonedVar.addHints(varExpr.getHints());
        return clonedVar;
    }

    @Override
    public UnaryExpr visit(UnaryExpr u, Void arg) throws CompilationException {
        UnaryExpr copy = new UnaryExpr(u.getExprType(), (Expression) u.getExpr().accept(this, arg));
        copy.setSourceLocation(u.getSourceLocation());
        copy.addHints(u.getHints());
        return copy;
    }

    @Override
    public FieldAccessor visit(FieldAccessor fa, Void arg) throws CompilationException {
        FieldAccessor copy = new FieldAccessor((Expression) fa.getExpr().accept(this, arg), fa.getIdent());
        copy.setSourceLocation(fa.getSourceLocation());
        copy.addHints(fa.getHints());
        return copy;
    }

    @Override
    public Expression visit(IndexAccessor ia, Void arg) throws CompilationException {
        Expression expr = (Expression) ia.getExpr().accept(this, arg);
        Expression indexExpr = null;
        if (ia.getIndexExpr() != null) {
            indexExpr = (Expression) ia.getIndexExpr().accept(this, arg);
        }
        IndexAccessor copy = new IndexAccessor(expr, indexExpr);
        copy.setSourceLocation(ia.getSourceLocation());
        copy.addHints(ia.getHints());
        return copy;
    }

    @Override
    public Expression visit(ListSliceExpression expression, Void arg) throws CompilationException {
        Expression expr = (Expression) expression.getExpr().accept(this, arg);
        Expression startIndexExpression = (Expression) expression.getStartIndexExpression().accept(this, arg);

        // End index expression can be null (optional)
        Expression endIndexExpression = null;
        if (expression.hasEndExpression()) {
            endIndexExpression = (Expression) expression.getEndIndexExpression().accept(this, arg);
        }
        ListSliceExpression copy = new ListSliceExpression(expr, startIndexExpression, endIndexExpression);
        copy.setSourceLocation(expression.getSourceLocation());
        copy.addHints(expression.getHints());
        return copy;
    }

    @Override
    public ILangExpression visit(CaseExpression caseExpr, Void arg) throws CompilationException {
        Expression conditionExpr = (Expression) caseExpr.getConditionExpr().accept(this, arg);
        List<Expression> whenExprList = copyExprList(caseExpr.getWhenExprs(), arg);
        List<Expression> thenExprList = copyExprList(caseExpr.getThenExprs(), arg);
        Expression elseExpr = (Expression) caseExpr.getElseExpr().accept(this, arg);
        CaseExpression copy = new CaseExpression(conditionExpr, whenExprList, thenExprList, elseExpr);
        copy.setSourceLocation(caseExpr.getSourceLocation());
        copy.addHints(caseExpr.getHints());
        return copy;
    }

    @Override
    public ILangExpression visit(WindowExpression winExpr, Void arg) throws CompilationException {
        List<Expression> newExprList = copyExprList(winExpr.getExprList(), arg);
        List<Expression> newPartitionList =
                winExpr.hasPartitionList() ? copyExprList(winExpr.getPartitionList(), arg) : null;
        List<Expression> newOrderbyList = winExpr.hasOrderByList() ? copyExprList(winExpr.getOrderbyList(), arg) : null;
        List<OrderbyClause.OrderModifier> newOrderbyModifierList =
                winExpr.hasOrderByList() ? new ArrayList<>(winExpr.getOrderbyModifierList()) : null;
        Expression newFrameStartExpr =
                winExpr.hasFrameStartExpr() ? (Expression) winExpr.getFrameStartExpr().accept(this, arg) : null;
        Expression newFrameEndExpr =
                winExpr.hasFrameEndExpr() ? (Expression) winExpr.getFrameEndExpr().accept(this, arg) : null;
        VariableExpr newWindowVar =
                winExpr.hasWindowVar() ? (VariableExpr) winExpr.getWindowVar().accept(this, arg) : null;
        List<Pair<Expression, Identifier>> newWindowFieldList =
                winExpr.hasWindowFieldList() ? copyFieldList(winExpr.getWindowFieldList(), arg) : null;
        WindowExpression copy = new WindowExpression(winExpr.getFunctionSignature(), newExprList, newPartitionList,
                newOrderbyList, newOrderbyModifierList, winExpr.getFrameMode(), winExpr.getFrameStartKind(),
                newFrameStartExpr, winExpr.getFrameEndKind(), newFrameEndExpr, winExpr.getFrameExclusionKind(),
                newWindowVar, newWindowFieldList, winExpr.getIgnoreNulls(), winExpr.getFromLast());
        copy.setSourceLocation(winExpr.getSourceLocation());
        copy.addHints(winExpr.getHints());
        return copy;
    }

    private List<Expression> copyExprList(List<Expression> exprs, Void arg) throws CompilationException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : exprs) {
            newExprList.add((Expression) expr.accept(this, arg));
        }
        return newExprList;
    }

    private List<Pair<Expression, Identifier>> copyFieldList(List<Pair<Expression, Identifier>> fieldList, Void arg)
            throws CompilationException {
        List<Pair<Expression, Identifier>> newFieldList = new ArrayList<>(fieldList.size());
        for (Pair<Expression, Identifier> field : fieldList) {
            newFieldList.add(new Pair<>((Expression) field.first.accept(this, arg), field.second));
        }
        return newFieldList;
    }
}
