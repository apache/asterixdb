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

import org.apache.asterix.common.exceptions.AsterixException;
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
import org.apache.asterix.lang.sqlpp.expression.IndependentSubquery;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class DeepCopyVisitor extends AbstractSqlppQueryExpressionVisitor<ILangExpression, Void> {

    @Override
    public FromClause visit(FromClause fromClause, Void arg) throws AsterixException {
        List<FromTerm> fromTerms = new ArrayList<>();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerms.add((FromTerm) fromTerm.accept(this, arg));
        }
        return new FromClause(fromTerms);
    }

    @Override
    public FromTerm visit(FromTerm fromTerm, Void arg) throws AsterixException {
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
        return new FromTerm(fromExpr, fromVar, positionVar, correlateClauses);
    }

    @Override
    public JoinClause visit(JoinClause joinClause, Void arg) throws AsterixException {
        Expression rightExpression = (Expression) joinClause.getRightExpression().accept(this, arg);
        VariableExpr rightVar = (VariableExpr) joinClause.getRightVariable().accept(this, arg);
        VariableExpr rightPositionVar = joinClause.getPositionalVariable() == null ? null
                : (VariableExpr) joinClause.getPositionalVariable().accept(this, arg);
        Expression conditionExpresion = (Expression) joinClause.getConditionExpression().accept(this, arg);
        return new JoinClause(joinClause.getJoinType(), rightExpression, rightVar, rightPositionVar,
                conditionExpresion);
    }

    @Override
    public NestClause visit(NestClause nestClause, Void arg) throws AsterixException {
        Expression rightExpression = (Expression) nestClause.getRightExpression().accept(this, arg);
        VariableExpr rightVar = (VariableExpr) nestClause.getRightVariable().accept(this, arg);
        VariableExpr rightPositionVar = nestClause.getPositionalVariable() == null ? null
                : (VariableExpr) nestClause.getPositionalVariable().accept(this, arg);
        Expression conditionExpresion = (Expression) nestClause.getConditionExpression().accept(this, arg);
        return new NestClause(nestClause.getJoinType(), rightExpression, rightVar, rightPositionVar,
                conditionExpresion);
    }

    @Override
    public UnnestClause visit(UnnestClause unnestClause, Void arg) throws AsterixException {
        Expression rightExpression = (Expression) unnestClause.getRightExpression().accept(this, arg);
        VariableExpr rightVar = (VariableExpr) unnestClause.getRightVariable().accept(this, arg);
        VariableExpr rightPositionVar = unnestClause.getPositionalVariable() == null ? null
                : (VariableExpr) unnestClause.getPositionalVariable().accept(this, arg);
        return new UnnestClause(unnestClause.getJoinType(), rightExpression, rightVar, rightPositionVar);
    }

    @Override
    public Projection visit(Projection projection, Void arg) throws AsterixException {
        return new Projection(projection.star() ? null : (Expression) projection.getExpression().accept(this, arg),
                projection.getName(),
                projection.star(), projection.exprStar());
    }

    @Override
    public SelectBlock visit(SelectBlock selectBlock, Void arg) throws AsterixException {
        FromClause fromClause = null;
        List<LetClause> letClauses = new ArrayList<>();
        WhereClause whereClause = null;
        GroupbyClause gbyClause = null;
        List<LetClause> gbyLetClauses = new ArrayList<>();
        HavingClause havingClause = null;
        SelectClause selectCluase;
        // Traverses the select block in the order of "from", "let"s, "where",
        // "group by", "let"s, "having" and "select".
        if (selectBlock.hasFromClause()) {
            fromClause = (FromClause) selectBlock.getFromClause().accept(this, arg);
        }
        if (selectBlock.hasLetClauses()) {
            List<LetClause> letList = selectBlock.getLetList();
            for (LetClause letClause : letList) {
                letClauses.add((LetClause) letClause.accept(this, arg));
            }
        }
        if (selectBlock.hasWhereClause()) {
            whereClause = (WhereClause) selectBlock.getWhereClause().accept(this, arg);
        }
        if (selectBlock.hasGroupbyClause()) {
            gbyClause = (GroupbyClause) selectBlock.getGroupbyClause().accept(this, arg);
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            List<LetClause> letListAfterGby = selectBlock.getLetListAfterGroupby();
            for (LetClause letClauseAfterGby : letListAfterGby) {
                gbyLetClauses.add((LetClause) letClauseAfterGby.accept(this, arg));
            }
        }
        if (selectBlock.hasHavingClause()) {
            havingClause = (HavingClause) selectBlock.getHavingClause().accept(this, arg);
        }
        selectCluase = (SelectClause) selectBlock.getSelectClause().accept(this, arg);
        return new SelectBlock(selectCluase, fromClause, letClauses, whereClause, gbyClause, gbyLetClauses,
                havingClause);
    }

    @Override
    public SelectClause visit(SelectClause selectClause, Void arg) throws AsterixException {
        SelectElement selectElement = null;
        SelectRegular selectRegular = null;
        if (selectClause.selectElement()) {
            selectElement = (SelectElement) selectClause.getSelectElement().accept(this, arg);
        }
        if (selectClause.selectRegular()) {
            selectRegular = (SelectRegular) selectClause.getSelectRegular().accept(this, arg);
        }
        return new SelectClause(selectElement, selectRegular, selectClause.distinct());
    }

    @Override
    public SelectElement visit(SelectElement selectElement, Void arg) throws AsterixException {
        return new SelectElement((Expression) selectElement.getExpression().accept(this, arg));
    }

    @Override
    public SelectRegular visit(SelectRegular selectRegular, Void arg) throws AsterixException {
        List<Projection> projections = new ArrayList<>();
        for (Projection projection : selectRegular.getProjections()) {
            projections.add((Projection) projection.accept(this, arg));
        }
        return new SelectRegular(projections);
    }

    @Override
    public SelectSetOperation visit(SelectSetOperation selectSetOperation, Void arg) throws AsterixException {
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
        return new SelectSetOperation(newLeftInput, rightInputs);
    }

    @Override
    public HavingClause visit(HavingClause havingClause, Void arg) throws AsterixException {
        return new HavingClause((Expression) havingClause.getFilterExpression().accept(this, arg));
    }

    @Override
    public Query visit(Query q, Void arg) throws AsterixException {
        return new Query(q.isExplain(), q.isTopLevel(), (Expression) q.getBody().accept(this, arg), q.getVarCounter(),
                q.getDataverses(), q.getDatasets());
    }

    @Override
    public FunctionDecl visit(FunctionDecl fd, Void arg) throws AsterixException {
        return new FunctionDecl(fd.getSignature(), fd.getParamList(), (Expression) fd.getFuncBody().accept(this, arg));
    }

    @Override
    public WhereClause visit(WhereClause whereClause, Void arg) throws AsterixException {
        return new WhereClause((Expression) whereClause.getWhereExpr().accept(this, arg));
    }

    @Override
    public OrderbyClause visit(OrderbyClause oc, Void arg) throws AsterixException {
        List<Expression> newOrderbyList = new ArrayList<>();
        for (Expression orderExpr : oc.getOrderbyList()) {
            newOrderbyList.add((Expression) orderExpr.accept(this, arg));
        }
        return new OrderbyClause(newOrderbyList, oc.getModifierList());
    }

    @Override
    public GroupbyClause visit(GroupbyClause gc, Void arg) throws AsterixException {
        List<GbyVariableExpressionPair> gbyPairList = new ArrayList<>();
        List<GbyVariableExpressionPair> decorPairList = new ArrayList<>();
        Map<Expression, VariableExpr> withVarMap = new HashMap<>();
        VariableExpr groupVarExpr = null;
        List<Pair<Expression, Identifier>> groupFieldList = new ArrayList<>();
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            VariableExpr var = gbyVarExpr.getVar();
            gbyPairList.add(new GbyVariableExpressionPair(var == null ? null : (VariableExpr) var.accept(this, arg),
                    (Expression) gbyVarExpr.getExpr().accept(this, arg)));
        }
        for (GbyVariableExpressionPair gbyVarExpr : gc.getDecorPairList()) {
            VariableExpr var = gbyVarExpr.getVar();
            decorPairList.add(new GbyVariableExpressionPair(var == null ? null : (VariableExpr) var.accept(this, arg),
                    (Expression) gbyVarExpr.getExpr().accept(this, arg)));
        }
        for (Entry<Expression, VariableExpr> entry : gc.getWithVarMap().entrySet()) {
            withVarMap.put((Expression) entry.getKey().accept(this, arg),
                    (VariableExpr) entry.getValue().accept(this, arg));
        }
        if (gc.hasGroupVar()) {
            groupVarExpr = (VariableExpr) gc.getGroupVar().accept(this, arg);
        }
        for (Pair<Expression, Identifier> field : gc.getGroupFieldList()) {
            groupFieldList.add(new Pair<>((Expression) field.first.accept(this, arg), field.second));
        }
        return new GroupbyClause(gbyPairList, decorPairList, withVarMap, groupVarExpr, groupFieldList,
                gc.hasHashGroupByHint(), gc.isGroupAll());
    }

    @Override
    public LimitClause visit(LimitClause limitClause, Void arg) throws AsterixException {
        Expression limitExpr = (Expression) limitClause.getLimitExpr().accept(this, arg);
        Expression offsetExpr = limitClause.hasOffset() ? (Expression) limitClause.getOffset().accept(this, arg) : null;
        return new LimitClause(limitExpr, offsetExpr);
    }

    @Override
    public LetClause visit(LetClause letClause, Void arg) throws AsterixException {
        return new LetClause((VariableExpr) letClause.getVarExpr().accept(this, arg),
                (Expression) letClause.getBindingExpr().accept(this, arg));
    }

    @Override
    public SelectExpression visit(SelectExpression selectExpression, Void arg) throws AsterixException {
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
            List<Expression> orderExprs = new ArrayList<>();
            for (Expression orderExpr : selectExpression.getOrderbyClause().getOrderbyList()) {
                orderExprs.add((Expression) orderExpr.accept(this, arg));
            }
            orderby = new OrderbyClause(orderExprs, selectExpression.getOrderbyClause().getModifierList());
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            limit = (LimitClause) selectExpression.getLimitClause().accept(this, arg);
        }
        return new SelectExpression(lets, select, orderby, limit, selectExpression.isSubquery());
    }

    @Override
    public LiteralExpr visit(LiteralExpr l, Void arg) throws AsterixException {
        return l;
    }

    @Override
    public ListConstructor visit(ListConstructor lc, Void arg) throws AsterixException {
        return new ListConstructor(lc.getType(), copyExprList(lc.getExprList(), arg));
    }

    @Override
    public RecordConstructor visit(RecordConstructor rc, Void arg) throws AsterixException {
        List<FieldBinding> bindings = new ArrayList<>();
        for (FieldBinding binding : rc.getFbList()) {
            FieldBinding fb = new FieldBinding((Expression) binding.getLeftExpr().accept(this, arg),
                    (Expression) binding.getRightExpr().accept(this, arg));
            bindings.add(fb);
        }
        return new RecordConstructor(bindings);
    }

    @Override
    public OperatorExpr visit(OperatorExpr operatorExpr, Void arg) throws AsterixException {
        return new OperatorExpr(copyExprList(operatorExpr.getExprList(), arg), operatorExpr.getExprBroadcastIdx(),
                operatorExpr.getOpList(), operatorExpr.isCurrentop());
    }

    @Override
    public IfExpr visit(IfExpr ifExpr, Void arg) throws AsterixException {
        Expression conditionExpr = (Expression) ifExpr.getCondExpr().accept(this, arg);
        Expression thenExpr = (Expression) ifExpr.getThenExpr().accept(this, arg);
        Expression elseExpr = (Expression) ifExpr.getElseExpr().accept(this, arg);
        return new IfExpr(conditionExpr, thenExpr, elseExpr);
    }

    @Override
    public QuantifiedExpression visit(QuantifiedExpression qe, Void arg) throws AsterixException {
        List<QuantifiedPair> quantifiedPairs = new ArrayList<>();
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            Expression expr = (Expression) pair.getExpr().accept(this, arg);
            VariableExpr var = (VariableExpr) pair.getVarExpr().accept(this, arg);
            quantifiedPairs.add(new QuantifiedPair(var, expr));
        }
        Expression condition = (Expression) qe.getSatisfiesExpr().accept(this, arg);
        return new QuantifiedExpression(qe.getQuantifier(), quantifiedPairs, condition);
    }

    @Override
    public CallExpr visit(CallExpr callExpr, Void arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : callExpr.getExprList()) {
            newExprList.add((Expression) expr.accept(this, arg));
        }
        return new CallExpr(callExpr.getFunctionSignature(), newExprList);
    }

    @Override
    public VariableExpr visit(VariableExpr varExpr, Void arg) throws AsterixException {
        VariableExpr clonedVar = new VariableExpr(new VarIdentifier(varExpr.getVar()));
        clonedVar.setIsNewVar(varExpr.getIsNewVar());
        return clonedVar;
    }

    @Override
    public UnaryExpr visit(UnaryExpr u, Void arg) throws AsterixException {
        return new UnaryExpr(u.getExprType(), (Expression) u.getExpr().accept(this, arg));
    }

    @Override
    public FieldAccessor visit(FieldAccessor fa, Void arg) throws AsterixException {
        return new FieldAccessor((Expression) fa.getExpr().accept(this, arg), fa.getIdent());
    }

    @Override
    public Expression visit(IndexAccessor ia, Void arg) throws AsterixException {
        Expression expr = (Expression) ia.getExpr().accept(this, arg);
        Expression indexExpr = null;
        if (ia.getIndexExpr() != null) {
            indexExpr = (Expression) ia.getIndexExpr().accept(this, arg);
        }
        return new IndexAccessor(expr, indexExpr);
    }

    @Override
    public ILangExpression visit(IndependentSubquery independentSubquery, Void arg) throws AsterixException {
        return new IndependentSubquery((Expression) independentSubquery.getExpr().accept(this, arg));
    }

    @Override
    public ILangExpression visit(CaseExpression caseExpr, Void arg) throws AsterixException {
        Expression conditionExpr = (Expression) caseExpr.getConditionExpr().accept(this, arg);
        List<Expression> whenExprList = copyExprList(caseExpr.getWhenExprs(), arg);
        List<Expression> thenExprList = copyExprList(caseExpr.getThenExprs(), arg);
        Expression elseExpr = (Expression) caseExpr.getElseExpr().accept(this, arg);
        return new CaseExpression(conditionExpr, whenExprList, thenExprList, elseExpr);
    }

    private List<Expression> copyExprList(List<Expression> exprs, Void arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : exprs) {
            newExprList.add((Expression) expr.accept(this, arg));
        }
        return newExprList;
    }

}
