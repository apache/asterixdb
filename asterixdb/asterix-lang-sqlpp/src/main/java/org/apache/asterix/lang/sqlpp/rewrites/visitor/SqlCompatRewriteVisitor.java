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

package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Applies initial rewritings for "SQL-compatible" evaluation mode
 * <ol>
 * <li>Rewrites {@code SELECT *} into {@code SELECT *.*}
 * <li>Rewrites {@code NOT? IN expr} into {@code NOT? IN to_array(expr)} if {@code expr} can return a non-list
 * </ol>
 * <p/>
 * Also applies subquery coercion rewritings as follows:
 * <ol>
 * <li> FROM/JOIN/UNNEST (subquery) --> no subquery coercion
 * <li> WITH/LET v = (subquery) --> no subquery coercion
 * <li> SOME v IN (subquery) --> no subquery coercion
 * <li> WHERE (x,y) = (subquery) --> coerce the subquery into a single array
 * <li> WHERE x IN (subquery) --> coerce the subquery into a collection of values
 * <li> WHERE (x,y) IN (subquery) --> coerce the subquery into a collection of arrays
 * <li> otherwise --> coerce the subquery into a single value
 * </ol>
 */
public final class SqlCompatRewriteVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final LangRewritingContext context;

    private final SelectSetOpInfo setOpInfo = new SelectSetOpInfo();

    public SqlCompatRewriteVisitor(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Expression visit(Projection projection, ILangExpression arg) throws CompilationException {
        if (projection.getKind() == Projection.Kind.STAR) {
            projection.setKind(Projection.Kind.EVERY_VAR_STAR);
        }
        return super.visit(projection, arg);
    }

    @Override
    public Expression visit(FromTerm fromTerm, ILangExpression arg) throws CompilationException {
        Expression expr = fromTerm.getLeftExpression();
        if (expr.getKind() == Expression.Kind.SELECT_EXPRESSION) {
            annotateSubqueryNoCoercion((SelectExpression) expr);
        }
        return super.visit(fromTerm, arg);
    }

    @Override
    public Expression visit(JoinClause joinClause, ILangExpression arg) throws CompilationException {
        Expression expr = joinClause.getRightExpression();
        if (expr.getKind() == Expression.Kind.SELECT_EXPRESSION) {
            annotateSubqueryNoCoercion((SelectExpression) expr);
        }
        return super.visit(joinClause, arg);
    }

    @Override
    public Expression visit(UnnestClause unnestClause, ILangExpression arg) throws CompilationException {
        Expression expr = unnestClause.getRightExpression();
        if (expr.getKind() == Expression.Kind.SELECT_EXPRESSION) {
            annotateSubqueryNoCoercion((SelectExpression) expr);
        }
        return super.visit(unnestClause, arg);
    }

    @Override
    public Expression visit(LetClause letClause, ILangExpression arg) throws CompilationException {
        Expression expr = letClause.getBindingExpr();
        if (expr.getKind() == Expression.Kind.SELECT_EXPRESSION) {
            annotateSubqueryNoCoercion((SelectExpression) expr);
        }
        return super.visit(letClause, arg);
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ILangExpression arg) throws CompilationException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            Expression expr = pair.getExpr();
            if (expr.getKind() == Expression.Kind.SELECT_EXPRESSION) {
                annotateSubqueryNoCoercion((SelectExpression) expr);
            }
        }
        return super.visit(qe, arg);
    }

    @Override
    public Expression visit(OperatorExpr opExpr, ILangExpression arg) throws CompilationException {
        List<OperatorType> opTypeList = opExpr.getOpList();
        if (opTypeList.size() == 1) {
            OperatorType opType = opTypeList.get(0);
            if (OperatorExpr.opIsComparison(opType)) {
                List<Expression> argList = opExpr.getExprList();
                Expression lhs = argList.get(0);
                Expression rhs = argList.get(1);
                if (lhs.getKind() == Expression.Kind.SELECT_EXPRESSION) {
                    annotateComparisonOpSubquery((SelectExpression) lhs, rhs);
                }
                if (rhs.getKind() == Expression.Kind.SELECT_EXPRESSION) {
                    annotateComparisonOpSubquery((SelectExpression) rhs, lhs);
                }
            } else if (opType == OperatorType.IN || opType == OperatorType.NOT_IN) {
                List<Expression> argList = opExpr.getExprList();
                Expression lhs = argList.get(0);
                Expression rhs = argList.get(1);
                switch (rhs.getKind()) {
                    case SELECT_EXPRESSION:
                        annotateInOpSubquery((SelectExpression) rhs, lhs);
                        break;
                    case LIST_CONSTRUCTOR_EXPRESSION:
                    case LIST_SLICE_EXPRESSION:
                        // NOT? IN [] -> keep as is
                        break;
                    default:
                        // NOT? IN expr -> NOT? IN to_array(expr)
                        List<Expression> newExprList = new ArrayList<>(2);
                        newExprList.add(lhs);
                        newExprList.add(createCallExpr(BuiltinFunctions.TO_ARRAY, rhs, opExpr.getSourceLocation()));
                        opExpr.setExprList(newExprList);
                        break;
                }
            }
        }
        return super.visit(opExpr, arg);
    }

    @Override
    public Expression visit(SelectExpression selectExpr, ILangExpression arg) throws CompilationException {
        SqlCompatSelectExpressionCoercionAnnotation selectExprAnn = null;
        if (selectExpr.isSubquery()) {
            selectExprAnn = selectExpr.findHint(SqlCompatSelectExpressionCoercionAnnotation.class);
            if (selectExprAnn == null) {
                // all other cases --> coerce the subquery into a scalar value
                if (annotateSubquery(selectExpr, SqlCompatSelectExpressionCoercionAnnotation.SCALAR,
                        SqlCompatSelectBlockCoercionAnnotation.SCALAR)) {
                    selectExprAnn = SqlCompatSelectExpressionCoercionAnnotation.SCALAR;
                }
            }
        }
        Expression newExpr = super.visit(selectExpr, arg);
        if (selectExprAnn != null) {
            newExpr = rewriteSelectExpression(newExpr, selectExprAnn);
        }
        return newExpr;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        super.visit(selectBlock, arg);
        SelectExpression selectExpr = (SelectExpression) arg;
        SqlCompatSelectBlockCoercionAnnotation selectBlockAnn =
                selectExpr.findHint(SqlCompatSelectBlockCoercionAnnotation.class);
        if (selectBlockAnn != null) {
            rewriteSelectBlock(selectBlock, selectBlockAnn);
        }
        return null;
    }

    private void annotateSubqueryNoCoercion(SelectExpression subqueryExpr) {
        // FROM/JOIN/UNNEST/LET (subquery) -> do NOT coerce the subquery
        subqueryExpr.addHint(SqlCompatSelectExpressionCoercionAnnotation.COLLECTION);
    }

    private void annotateComparisonOpSubquery(SelectExpression subqueryExpr, Expression otherArg)
            throws CompilationException {
        // (x,y) = (subquery) -> coerce the subquery into a single array
        // x = (subquery) -> coerce the subquery into a scalar value
        annotateSubquery(subqueryExpr, SqlCompatSelectExpressionCoercionAnnotation.SCALAR,
                getSelectBlockAnnotationForOpSubquery(otherArg));
    }

    private void annotateInOpSubquery(SelectExpression subqueryExpr, Expression otherArg) throws CompilationException {
        // (x,y) in (subquery) -> coerce the subquery into a collection of arrays
        // x in (subquery) -> coerce the subquery into a collection of scalar values
        annotateSubquery(subqueryExpr, SqlCompatSelectExpressionCoercionAnnotation.COLLECTION,
                getSelectBlockAnnotationForOpSubquery(otherArg));
    }

    private static SqlCompatSelectBlockCoercionAnnotation getSelectBlockAnnotationForOpSubquery(Expression otherArg)
            throws CompilationException {
        if (otherArg.getKind() == Expression.Kind.LIST_CONSTRUCTOR_EXPRESSION) {
            ListConstructor lc = (ListConstructor) otherArg;
            switch (lc.getType()) {
                case ORDERED_LIST_CONSTRUCTOR:
                    return SqlCompatSelectBlockCoercionAnnotation.ARRAY;
                case UNORDERED_LIST_CONSTRUCTOR:
                    return SqlCompatSelectBlockCoercionAnnotation.MULTISET;
                default:
                    throw new CompilationException(ErrorCode.ILLEGAL_STATE, otherArg.getSourceLocation(), "");
            }
        } else {
            return SqlCompatSelectBlockCoercionAnnotation.SCALAR;
        }
    }

    private boolean annotateSubquery(SelectExpression subqueryExpr,
            SqlCompatSelectExpressionCoercionAnnotation selectExprAnnotation,
            SqlCompatSelectBlockCoercionAnnotation selectBlockAnn) throws CompilationException {
        setOpInfo.reset();
        analyzeSelectSetOp(subqueryExpr.getSelectSetOperation(), setOpInfo);
        if (setOpInfo.subqueryExists) {
            throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                    subqueryExpr.getSourceLocation(), "");
        }
        if (setOpInfo.selectRegularExists) {
            if (setOpInfo.selectElementExists) {
                throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                        subqueryExpr.getSourceLocation(), "Both SELECT and SELECT VALUE are present");
            }
            subqueryExpr.addHint(selectExprAnnotation);
            subqueryExpr.addHint(selectBlockAnn);
            return true;
        } else {
            return false;
        }
    }

    private static void analyzeSelectSetOp(SelectSetOperation setOp, SelectSetOpInfo outSelectExprInfo)
            throws CompilationException {
        analyzeSelectSetOpInput(setOp.getLeftInput(), outSelectExprInfo);
        if (setOp.hasRightInputs()) {
            for (SetOperationRight rhs : setOp.getRightInputs()) {
                analyzeSelectSetOpInput(rhs.getSetOperationRightInput(), outSelectExprInfo);
            }
        }
    }

    private static void analyzeSelectSetOpInput(SetOperationInput setOpInput, SelectSetOpInfo outSelectSetOpInfo)
            throws CompilationException {
        if (setOpInput.selectBlock()) {
            SelectBlock selectBlock = setOpInput.getSelectBlock();
            SelectClause selectClause = selectBlock.getSelectClause();
            outSelectSetOpInfo.selectRegularExists |= selectClause.selectRegular();
            outSelectSetOpInfo.selectElementExists |= selectClause.selectElement();
        } else if (setOpInput.subquery()) {
            outSelectSetOpInfo.subqueryExists = true;
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "");
        }
    }

    private static CallExpr createCallExpr(FunctionIdentifier fid, Expression inExpr, SourceLocation sourceLoc) {
        List<Expression> argList = new ArrayList<>(1);
        argList.add(inExpr);
        CallExpr callExpr = new CallExpr(new FunctionSignature(fid), argList);
        callExpr.setSourceLocation(sourceLoc);
        return callExpr;
    }

    private static final class SelectSetOpInfo {
        private boolean subqueryExists;
        private boolean selectRegularExists;
        private boolean selectElementExists;

        void reset() {
            subqueryExists = false;
            selectRegularExists = false;
            selectElementExists = false;
        }
    }

    private enum SqlCompatSelectExpressionCoercionAnnotation implements IExpressionAnnotation {
        /**
         * Indicates that the result of the {@link SelectExpression}
         * must be coerced into a single item if its cardinality is 1 or to MISSING otherwise.
         */
        SCALAR,

        /**
         * Indicates that no transformation is needed
         */
        COLLECTION
    }

    private enum SqlCompatSelectBlockCoercionAnnotation implements IExpressionAnnotation {
        /**
         * Indicates that the output record of the {@link SelectBlock} must be transformed
         * into a scalar value if that output record has 1 field, or transformed into MISSING value otherwise
         */
        SCALAR,

        /**
         * Indicates that the output record of the {@link SelectBlock} must be transformed
         * into an array
         */
        ARRAY,

        /**
         * Indicates that the output record of the {@link SelectBlock} must be transformed
         * into a multiset
         */
        MULTISET
    }

    private void rewriteSelectBlock(SelectBlock selectBlock, SqlCompatSelectBlockCoercionAnnotation ann)
            throws CompilationException {
        SelectClause selectClause = selectBlock.getSelectClause();
        List<Projection> projectList = selectClause.getSelectRegular().getProjections();
        switch (ann) {
            case SCALAR:
                /*
                 * SELECT x -> SELECT VALUE x
                 * SELECT x, y -> ERROR
                 * SELECT * -> ERROR
                 */
                if (projectList.size() > 1) {
                    throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                            projectList.get(1).getSourceLocation(), "Subquery returns more than one field");
                }
                Projection projection = projectList.get(0);
                if (projection.getKind() != Projection.Kind.NAMED_EXPR) {
                    throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                            projection.getSourceLocation(), "Unsupported projection kind");
                }
                SelectElement selectElement = new SelectElement(projection.getExpression());
                selectElement.setSourceLocation(selectClause.getSourceLocation());
                selectClause.setSelectElement(selectElement);
                break;
            case ARRAY:
            case MULTISET:
                /*
                 * SELECT x -> SELECT VALUE [x]  (or SELECT VALUE {{x}})
                 * SELECT x, y -> SELECT VALUE [x, y] (or SELECT VALUE {{x, y}})
                 * SELECT * -> ERROR
                 */
                List<Expression> exprList = new ArrayList<>(projectList.size());
                for (Projection p : projectList) {
                    if (p.getKind() != Projection.Kind.NAMED_EXPR) {
                        throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                                p.getSourceLocation(), "Unsupported projection kind");
                    }
                    exprList.add(p.getExpression());
                }
                ListConstructor.Type listType = ann == SqlCompatSelectBlockCoercionAnnotation.ARRAY
                        ? ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR
                        : ListConstructor.Type.UNORDERED_LIST_CONSTRUCTOR;
                ListConstructor listExpr = new ListConstructor(listType, exprList);
                listExpr.setSourceLocation(selectClause.getSourceLocation());
                selectElement = new SelectElement(listExpr);
                selectElement.setSourceLocation(selectClause.getSourceLocation());
                selectClause.setSelectElement(selectElement);
                break;
            default:
                throw new CompilationException(ErrorCode.ILLEGAL_STATE, selectBlock.getSourceLocation(),
                        ann.toString());
        }
    }

    private Expression rewriteSelectExpression(Expression inExpr, SqlCompatSelectExpressionCoercionAnnotation ann)
            throws CompilationException {
        switch (ann) {
            case SCALAR:
                /*
                 * (SELECT ...)
                 * ->
                 * STRICT_FIRST_ELEMENT
                 * (
                 *  SELECT VALUE v2[(LEN(v2)-1)*2]
                 *  LET v2 = (SELECT VALUE v1 FROM (SELECT ...) v1 LIMIT 2)
                 * )
                 */
                SourceLocation sourceLoc = inExpr.getSourceLocation();

                /*
                 * E1: SELECT VALUE v1 FROM (SELECT ...) v1 LIMIT 2
                 */
                VarIdentifier v1 = context.newVariable();
                VariableExpr v1Ref1 = new VariableExpr(v1);
                v1Ref1.setSourceLocation(sourceLoc);
                FromTerm ft1 = new FromTerm(inExpr, v1Ref1, null, null);
                ft1.setSourceLocation(sourceLoc);
                List<FromTerm> fc1Terms = new ArrayList<>(1);
                fc1Terms.add(ft1);
                FromClause fc1 = new FromClause(fc1Terms);
                fc1.setSourceLocation(sourceLoc);
                VariableExpr v1Ref2 = new VariableExpr(v1);
                v1Ref2.setSourceLocation(sourceLoc);
                SelectElement sv1 = new SelectElement(v1Ref2);
                sv1.setSourceLocation(sourceLoc);
                SelectClause sc1 = new SelectClause(sv1, null, false);
                sc1.setSourceLocation(sourceLoc);
                SelectBlock sb1 = new SelectBlock(sc1, fc1, null, null, null);
                sv1.setSourceLocation(sourceLoc);
                SelectSetOperation sop1 = new SelectSetOperation(new SetOperationInput(sb1, null), null);
                sop1.setSourceLocation(sourceLoc);
                LimitClause lc1 = new LimitClause(new LiteralExpr(new IntegerLiteral(2)), null);
                lc1.setSourceLocation(sourceLoc);
                SelectExpression se1 = new SelectExpression(null, sop1, null, lc1, true);
                se1.setSourceLocation(sourceLoc);

                /*
                 * E2:
                 *   SELECT VALUE v2[(LEN(v2)-1)*2]
                 *   LET v2 = (..E1..)
                 *
                 * E2 returns {{ item }} if LEN(E1) == 1, otherwise it returns {{ MISSING }}
                 */
                VarIdentifier v2 = context.newVariable();
                VariableExpr v2Ref1 = new VariableExpr(v2);
                v2Ref1.setSourceLocation(sourceLoc);
                LetClause lc2 = new LetClause(v2Ref1, se1);
                lc2.setSourceLocation(sourceLoc);

                VariableExpr v2Ref2 = new VariableExpr(v2);
                v2Ref2.setSourceLocation(sourceLoc);
                List<Expression> lenArgs = new ArrayList<>(1);
                lenArgs.add(v2Ref2);
                CallExpr lenExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.LEN), lenArgs);
                lenExpr.setSourceLocation(sourceLoc);

                OperatorExpr minusExpr = new OperatorExpr();
                minusExpr.setCurrentop(true);
                minusExpr.addOperator(OperatorType.MINUS);
                minusExpr.addOperand(lenExpr);
                minusExpr.addOperand(new LiteralExpr(new IntegerLiteral(1)));
                minusExpr.setSourceLocation(sourceLoc);

                OperatorExpr mulExpr = new OperatorExpr();
                mulExpr.setCurrentop(true);
                mulExpr.addOperator(OperatorType.MUL);
                mulExpr.addOperand(minusExpr);
                mulExpr.addOperand(new LiteralExpr(new IntegerLiteral(2)));
                mulExpr.setSourceLocation(sourceLoc);

                VariableExpr v2Ref3 = new VariableExpr(v2);
                v2Ref3.setSourceLocation(sourceLoc);
                IndexAccessor iaExpr = new IndexAccessor(v2Ref3, IndexAccessor.IndexKind.ELEMENT, mulExpr);
                iaExpr.setSourceLocation(sourceLoc);

                SelectElement sv2 = new SelectElement(iaExpr);
                sv2.setSourceLocation(sourceLoc);
                SelectClause sc2 = new SelectClause(sv2, null, false);
                sc2.setSourceLocation(sourceLoc);
                List<AbstractClause> sb2Clauses = new ArrayList<>(1);
                sb2Clauses.add(lc2);
                SelectBlock sb2 = new SelectBlock(sc2, null, sb2Clauses, null, null);
                sb2.setSourceLocation(sourceLoc);
                SelectSetOperation sop2 = new SelectSetOperation(new SetOperationInput(sb2, null), null);
                sop2.setSourceLocation(sourceLoc);
                SelectExpression se2 = new SelectExpression(null, sop2, null, null, true);
                se2.setSourceLocation(sourceLoc);

                /*
                 * E3: STRICT_FIRST_ELEMENT(..E2..)
                 */
                List<Expression> firstElemArgs = new ArrayList<>(1);
                firstElemArgs.add(se2);
                FunctionIdentifier firstElemFun =
                        FunctionMapUtil.createCoreAggregateFunctionIdentifier(BuiltinFunctions.SCALAR_FIRST_ELEMENT);
                CallExpr firstElemExpr = new CallExpr(new FunctionSignature(firstElemFun), firstElemArgs);
                firstElemExpr.setSourceLocation(sourceLoc);
                return firstElemExpr;
            case COLLECTION:
                // indicates that no transformation is necessary
                return inExpr;
            default:
                throw new CompilationException(ErrorCode.ILLEGAL_STATE, inExpr.getSourceLocation(), ann.toString());
        }
    }
}
