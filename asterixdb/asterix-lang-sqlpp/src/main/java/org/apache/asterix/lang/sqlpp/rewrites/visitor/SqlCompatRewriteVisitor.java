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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
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
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
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
 * <li> SOME/EVERY v IN (subquery) --> no subquery coercion
 * <li> [NOT] EXISTS (subquery) --> no subquery coercion
 * <li> WHERE (x,y) = (subquery) --> coerce the subquery into a single array
 * <li> WHERE x IN (subquery) --> coerce the subquery into a collection of values
 * <li> WHERE (x,y) IN (subquery) --> coerce the subquery into a collection of arrays
 * <li> otherwise --> coerce the subquery into a single value
 * </ol>
 */
public final class SqlCompatRewriteVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final LangRewritingContext context;

    private final SelectExpressionAnalyzer selectExprAnalyzer = new SelectExpressionAnalyzer();

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
        switch (joinClause.getJoinType()) {
            case LEFTOUTER:
            case RIGHTOUTER:
                joinClause.setOuterJoinMissingValueType(Literal.Type.NULL);
                break;
            case INNER:
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, joinClause.getSourceLocation(),
                        String.valueOf(joinClause.getJoinType()));
        }
        return super.visit(joinClause, arg);
    }

    @Override
    public Expression visit(UnnestClause unnestClause, ILangExpression arg) throws CompilationException {
        Expression expr = unnestClause.getRightExpression();
        if (expr.getKind() == Expression.Kind.SELECT_EXPRESSION) {
            annotateSubqueryNoCoercion((SelectExpression) expr);
        }
        // keep UNNEST clause aligned with JOIN clause when it comes to producing NULL values
        switch (unnestClause.getUnnestType()) {
            case LEFTOUTER:
                unnestClause.setOuterUnnestMissingValueType(Literal.Type.NULL);
                break;
            case INNER:
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, unnestClause.getSourceLocation(),
                        String.valueOf(unnestClause.getUnnestType()));
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
    public Expression visit(UnaryExpr u, ILangExpression arg) throws CompilationException {
        switch (u.getExprType()) {
            case EXISTS:
            case NOT_EXISTS:
                Expression expr = u.getExpr();
                if (expr.getKind() == Expression.Kind.SELECT_EXPRESSION) {
                    annotateSubqueryNoCoercion((SelectExpression) expr);
                }
                break;
        }
        return super.visit(u, arg);
    }

    @Override
    public Expression visit(SelectExpression selectExpr, ILangExpression arg) throws CompilationException {
        SqlCompatSelectExpressionCoercionAnnotation selectExprAnn = null;
        if (selectExpr.isSubquery()) {
            selectExprAnn = selectExpr.findHint(SqlCompatSelectExpressionCoercionAnnotation.class);
            if (selectExprAnn == null) {
                // all other cases --> coerce the subquery into a scalar value
                selectExprAnn = annotateSubquery(selectExpr, SqlCompatSelectCoercionKind.SCALAR,
                        SqlCompatSelectCoercionKind.SCALAR);
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
        SqlCompatSelectExpressionCoercionAnnotation selectExprAnn =
                selectExpr.findHint(SqlCompatSelectExpressionCoercionAnnotation.class);
        if (selectExprAnn != null) {
            rewriteSelectBlock(selectBlock, selectExprAnn);
        }
        return null;
    }

    private void annotateSubqueryNoCoercion(SelectExpression subqueryExpr) {
        // FROM/JOIN/UNNEST/LET (subquery) -> do NOT coerce the subquery
        subqueryExpr.addHint(SqlCompatSelectExpressionCoercionAnnotation.NONE_NONE);
    }

    private void annotateComparisonOpSubquery(SelectExpression subqueryExpr, Expression otherArg)
            throws CompilationException {
        // (x,y) = (subquery) -> coerce the subquery into a single array
        // x = (subquery) -> coerce the subquery into a scalar value
        annotateSubquery(subqueryExpr, SqlCompatSelectCoercionKind.SCALAR,
                getSelectBlockAnnotationForOpSubquery(otherArg));
    }

    private void annotateInOpSubquery(SelectExpression subqueryExpr, Expression otherArg) throws CompilationException {
        // (x,y) in (subquery) -> coerce the subquery into a collection of arrays
        // x in (subquery) -> coerce the subquery into a collection of scalar values
        annotateSubquery(subqueryExpr, SqlCompatSelectCoercionKind.NONE,
                getSelectBlockAnnotationForOpSubquery(otherArg));
    }

    private static SqlCompatSelectCoercionKind getSelectBlockAnnotationForOpSubquery(Expression otherArg)
            throws CompilationException {
        if (otherArg.getKind() == Expression.Kind.LIST_CONSTRUCTOR_EXPRESSION) {
            ListConstructor lc = (ListConstructor) otherArg;
            switch (lc.getType()) {
                case ORDERED_LIST_CONSTRUCTOR:
                    return SqlCompatSelectCoercionKind.ARRAY;
                case UNORDERED_LIST_CONSTRUCTOR:
                    return SqlCompatSelectCoercionKind.MULTISET;
                default:
                    throw new CompilationException(ErrorCode.ILLEGAL_STATE, otherArg.getSourceLocation(), "");
            }
        } else {
            return SqlCompatSelectCoercionKind.SCALAR;
        }
    }

    private SqlCompatSelectExpressionCoercionAnnotation annotateSubquery(SelectExpression subqueryExpr,
            SqlCompatSelectCoercionKind cardinalityCoercion, SqlCompatSelectCoercionKind typeCoercion)
            throws CompilationException {
        selectExprAnalyzer.analyze(subqueryExpr.getSelectSetOperation(), true);
        if (selectExprAnalyzer.subqueryExists) {
            throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                    subqueryExpr.getSourceLocation(), "");
        }
        if (selectExprAnalyzer.selectRegularExists) {
            if (selectExprAnalyzer.selectElementExists) {
                throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                        subqueryExpr.getSourceLocation(), "Both SELECT and SELECT VALUE are present");
            }
            String typeCoercionFieldName = typeCoercion == SqlCompatSelectCoercionKind.NONE ? null
                    : selectExprAnalyzer.generateFieldName(context);
            SqlCompatSelectExpressionCoercionAnnotation ann = new SqlCompatSelectExpressionCoercionAnnotation(
                    cardinalityCoercion, typeCoercion, typeCoercionFieldName);
            subqueryExpr.addHint(ann);
            return ann;
        } else {
            return null;
        }
    }

    @Override
    public Expression visit(SelectSetOperation setOp, ILangExpression arg) throws CompilationException {
        // let subquery coercion rewriting run first
        super.visit(setOp, arg);

        if (setOp.hasRightInputs()) {
            // SetOp (UNION ALL) rewriting
            selectExprAnalyzer.analyze(setOp, false);
            if (selectExprAnalyzer.subqueryExists) {
                throw new CompilationException(ErrorCode.COMPILATION_SET_OPERATION_ERROR, setOp.getSourceLocation(),
                        setOp.getRightInputs().get(0).getSetOpType().toString(), "");
            }
            if (selectExprAnalyzer.selectRegularExists) {
                if (selectExprAnalyzer.selectElementExists) {
                    throw new CompilationException(ErrorCode.COMPILATION_SET_OPERATION_ERROR, setOp.getSourceLocation(),
                            setOp.getRightInputs().get(0).getSetOpType().toString(),
                            "Both SELECT and SELECT VALUE are present");
                }
                rewriteSelectSetOp(setOp);
            }
        }

        return null;
    }

    private void rewriteSelectSetOp(SelectSetOperation setOp) throws CompilationException {
        /*
         * SELECT a, b, c FROM ...
         * UNION ALL
         * SELECT d, e, f FROM ....
         * -->
         * SELECT a, b, c
         * UNION ALL
         * SELECT v.d AS a, v.e AS b, v.f AS c FROM ( SELECT d, e, f FROM ... )
         */
        SelectBlock leftSelectBlock = setOp.getLeftInput().getSelectBlock();
        boolean ok = leftSelectBlock != null && setOp.hasRightInputs();
        if (!ok) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, setOp.getSourceLocation(), "");
        }
        // collect all field names from the left input
        List<String> leftFieldNames = collectFieldNames(leftSelectBlock, setOp.getRightInputs().get(0).getSetOpType());
        for (SetOperationRight rightInput : setOp.getRightInputs()) {
            // rewrite right inputs
            rewriteSelectSetOpRightInput(rightInput, leftFieldNames, setOp.getSourceLocation());
        }
    }

    private void rewriteSelectSetOpRightInput(SetOperationRight setOpRight, List<String> outputFieldNames,
            SourceLocation sourceLoc) throws CompilationException {
        SetOperationInput setOpRightInput = setOpRight.getSetOperationRightInput();
        SelectBlock rightSelectBlock = setOpRightInput.getSelectBlock();
        if (rightSelectBlock == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, "");
        }
        List<String> inputFieldNames = collectFieldNames(rightSelectBlock, setOpRight.getSetOpType());
        int nFields = inputFieldNames.size();
        if (nFields != outputFieldNames.size()) {
            throw new CompilationException(ErrorCode.COMPILATION_SET_OPERATION_ERROR, sourceLoc,
                    setOpRight.getSetOpType().toString(), "Unequal number of input fields");
        }

        SelectSetOperation setOp1 = new SelectSetOperation(new SetOperationInput(rightSelectBlock, null), null);
        setOp1.setSourceLocation(sourceLoc);
        SelectExpression selectExpr1 = new SelectExpression(null, setOp1, null, null, true);
        selectExpr1.setSourceLocation(sourceLoc);

        VarIdentifier v1 = context.newVariable();
        VariableExpr v1Expr1 = new VariableExpr(v1);
        v1Expr1.setSourceLocation(sourceLoc);

        FromTerm fromTerm1 = new FromTerm(selectExpr1, v1Expr1, null, null);
        fromTerm1.setSourceLocation(sourceLoc);
        List<FromTerm> fromTermList1 = new ArrayList<>(1);
        fromTermList1.add(fromTerm1);
        FromClause fromClause1 = new FromClause(fromTermList1);
        fromClause1.setSourceLocation(sourceLoc);

        List<FieldBinding> fb1List = new ArrayList<>(nFields);
        for (int i = 0; i < nFields; i++) {
            VariableExpr v1Expr2 = new VariableExpr(v1);
            v1Expr2.setSourceLocation(sourceLoc);
            FieldAccessor fa1 = new FieldAccessor(v1Expr2, new Identifier(inputFieldNames.get(i)));
            fa1.setSourceLocation(sourceLoc);
            LiteralExpr lit1 = new LiteralExpr(new StringLiteral(outputFieldNames.get(i)));
            lit1.setSourceLocation(sourceLoc);
            fb1List.add(new FieldBinding(lit1, fa1));
        }
        RecordConstructor rc1 = new RecordConstructor(fb1List);
        rc1.setSourceLocation(sourceLoc);
        SelectClause selectClause1 = new SelectClause(new SelectElement(rc1), null, false);
        selectClause1.setSourceLocation(sourceLoc);
        SelectBlock newRightSelectBlock = new SelectBlock(selectClause1, fromClause1, null, null, null);
        newRightSelectBlock.setSourceLocation(sourceLoc);
        setOpRightInput.setSelectBlock(newRightSelectBlock);
    }

    private List<String> collectFieldNames(SelectBlock selectBlock, SetOpType setOpType) throws CompilationException {
        SelectRegular selectRegular = selectBlock.getSelectClause().getSelectRegular();
        if (selectRegular == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectBlock.getSourceLocation(), "");
        }
        List<Projection> projectionList = selectRegular.getProjections();
        List<String> fieldNames = new ArrayList<>(projectionList.size());
        for (Projection projection : projectionList) {
            if (projection.getKind() != Projection.Kind.NAMED_EXPR) {
                throw new CompilationException(ErrorCode.COMPILATION_SET_OPERATION_ERROR,
                        projection.getSourceLocation(), setOpType.toString(), "Unsupported projection kind");
            }
            fieldNames.add(projection.getName());
        }
        return fieldNames;
    }

    private static CallExpr createCallExpr(FunctionIdentifier fid, Expression inExpr, SourceLocation sourceLoc) {
        List<Expression> argList = new ArrayList<>(1);
        argList.add(inExpr);
        CallExpr callExpr = new CallExpr(new FunctionSignature(fid), argList);
        callExpr.setSourceLocation(sourceLoc);
        return callExpr;
    }

    private static final class SelectExpressionAnalyzer {

        private boolean subqueryExists;
        private boolean selectRegularExists;
        private boolean selectElementExists;
        private boolean computeSelectRegularAllFields;
        private final Set<String> selectRegularAllFields = new HashSet<>();

        private void reset(boolean computeSelectRegularAllFields) {
            subqueryExists = false;
            selectRegularExists = false;
            selectElementExists = false;
            selectRegularAllFields.clear();
            this.computeSelectRegularAllFields = computeSelectRegularAllFields;
        }

        private void analyze(SelectSetOperation setOp, boolean computeSelectRegularAllFields)
                throws CompilationException {
            reset(computeSelectRegularAllFields);
            analyzeSelectSetOpInput(setOp.getLeftInput());
            if (setOp.hasRightInputs()) {
                for (SetOperationRight rhs : setOp.getRightInputs()) {
                    analyzeSelectSetOpInput(rhs.getSetOperationRightInput());
                }
            }
        }

        private void analyzeSelectSetOpInput(SetOperationInput setOpInput) throws CompilationException {
            if (setOpInput.selectBlock()) {
                SelectBlock selectBlock = setOpInput.getSelectBlock();
                SelectClause selectClause = selectBlock.getSelectClause();
                if (selectClause.selectRegular()) {
                    selectRegularExists = true;
                    if (computeSelectRegularAllFields) {
                        for (Projection projection : selectClause.getSelectRegular().getProjections()) {
                            if (projection.getKind() == Projection.Kind.NAMED_EXPR) {
                                selectRegularAllFields.add(projection.getName());
                            }
                        }
                    }
                } else if (selectClause.selectElement()) {
                    selectElementExists = true;
                }
            } else if (setOpInput.subquery()) {
                subqueryExists = true;
            } else {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "");
            }
        }

        private String generateFieldName(LangRewritingContext ctx) {
            String fieldName;
            do {
                fieldName = SqlppVariableUtil.variableNameToDisplayedFieldName(ctx.newVariable().getValue());
            } while (selectRegularAllFields.contains(fieldName));
            return fieldName;
        }
    }

    private static class SqlCompatSelectExpressionCoercionAnnotation implements IExpressionAnnotation {

        static final SqlCompatSelectExpressionCoercionAnnotation NONE_NONE =
                new SqlCompatSelectExpressionCoercionAnnotation(SqlCompatSelectCoercionKind.NONE,
                        SqlCompatSelectCoercionKind.NONE, null);

        final SqlCompatSelectCoercionKind cardinalityCoercion;

        final SqlCompatSelectCoercionKind typeCoercion;

        final String typeCoercionFieldName;

        SqlCompatSelectExpressionCoercionAnnotation(SqlCompatSelectCoercionKind cardinalityCoercion,
                SqlCompatSelectCoercionKind typeCoercion, String typeCoercionFieldName) {
            this.cardinalityCoercion = Objects.requireNonNull(cardinalityCoercion);
            this.typeCoercion = Objects.requireNonNull(typeCoercion);
            this.typeCoercionFieldName = typeCoercionFieldName;
        }
    }

    private enum SqlCompatSelectCoercionKind {

        /**
         * Indicates that no transformation is needed.
         * Applicable to both type and cardinality coercion.
         */
        NONE,

        /**
         * Used to specify type and cardinality coercion.
         * <p>
         * When used for type coercion
         * indicates that the output record of the {@link SelectBlock} must be transformed
         * into a scalar value if that output record has 1 field, or transformed into MISSING value otherwise.
         * <p>
         * When used for cardinality coercion
         * indicates that the result of the {@link SelectExpression}
         * must be coerced into a single item if its cardinality is 1 or to MISSING otherwise.
         */
        SCALAR,

        /**
         * Only used to specify type coercion.
         * <p>
         * Indicates that the output record of the {@link SelectBlock} must be transformed
         * into an array
         */
        ARRAY,

        /**
         * Only used to specify type coercion.
         * <p>
         * Indicates that the output record of the {@link SelectBlock} must be transformed
         * into a multiset
         */
        MULTISET,
    }

    private void rewriteSelectBlock(SelectBlock selectBlock, SqlCompatSelectExpressionCoercionAnnotation ann)
            throws CompilationException {
        SqlCompatSelectCoercionKind typeCoercion = ann.typeCoercion;
        switch (typeCoercion) {
            case SCALAR:
                /*
                 * SELECT x -> SELECT x, x AS $new_unique_field
                 * SELECT x, y -> ERROR
                 * SELECT * -> ERROR
                 */
                SelectClause selectClause = selectBlock.getSelectClause();
                List<Projection> projectList = selectClause.getSelectRegular().getProjections();
                if (projectList.size() > 1) {
                    throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                            projectList.get(1).getSourceLocation(), "Subquery returns more than one field");
                }
                Projection projection = projectList.get(0);
                if (projection.getKind() != Projection.Kind.NAMED_EXPR) {
                    throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                            projection.getSourceLocation(), "Unsupported projection kind");
                }
                Projection typeCoercionProj = new Projection(Projection.Kind.NAMED_EXPR,
                        (Expression) SqlppRewriteUtil.deepCopy(projection.getExpression()), ann.typeCoercionFieldName);
                projectList.add(typeCoercionProj);
                break;
            case ARRAY:
            case MULTISET:
                /*
                 * SELECT x -> SELECT x, [x] AS $new_unique_field -- for ARRAY case
                 *         (or SELECT VALUE x, {{x}} AS $new_unique_field) -- for MULTISET case
                 * SELECT x, y -> SELECT x, y, [x, y] AS $new_unique_field -- for ARRAY case
                 *            (or SELECT x, y, {{x, y}} AS $new_unique_field) -- for MULTISET case
                 * SELECT * -> ERROR
                 */
                selectClause = selectBlock.getSelectClause();
                projectList = selectClause.getSelectRegular().getProjections();
                List<Expression> exprList = new ArrayList<>(projectList.size());
                for (Projection p : projectList) {
                    if (p.getKind() != Projection.Kind.NAMED_EXPR) {
                        throw new CompilationException(ErrorCode.COMPILATION_SUBQUERY_COERCION_ERROR,
                                p.getSourceLocation(), "Unsupported projection kind");
                    }
                    exprList.add((Expression) SqlppRewriteUtil.deepCopy(p.getExpression()));
                }
                ListConstructor.Type listType = typeCoercion == SqlCompatSelectCoercionKind.ARRAY
                        ? ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR
                        : ListConstructor.Type.UNORDERED_LIST_CONSTRUCTOR;
                ListConstructor listExpr = new ListConstructor(listType, exprList);
                listExpr.setSourceLocation(selectClause.getSourceLocation());
                typeCoercionProj = new Projection(Projection.Kind.NAMED_EXPR, listExpr, ann.typeCoercionFieldName);
                projectList.add(typeCoercionProj);
                break;
            case NONE:
                break;
            default:
                throw new CompilationException(ErrorCode.ILLEGAL_STATE, selectBlock.getSourceLocation(),
                        ann.toString());
        }
    }

    private Expression rewriteSelectExpression(Expression inExpr, SqlCompatSelectExpressionCoercionAnnotation ann)
            throws CompilationException {
        SourceLocation sourceLoc = inExpr.getSourceLocation();

        if (ann.typeCoercion != SqlCompatSelectCoercionKind.NONE) {
            /*
             * inExpr = SELECT ..., type_coercion_expr AS $new_unique_field
             * -->
             * inExpr = SELECT VALUE v1.$new_unique_field FROM (SELECT ..., type_coercion_expr AS $new_unique_field) v1
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
            FieldAccessor fa = new FieldAccessor(v1Ref2, new Identifier(ann.typeCoercionFieldName));
            fa.setSourceLocation(sourceLoc);
            SelectElement sv1 = new SelectElement(fa);
            sv1.setSourceLocation(sourceLoc);
            SelectClause sc1 = new SelectClause(sv1, null, false);
            sc1.setSourceLocation(sourceLoc);
            SelectBlock sb1 = new SelectBlock(sc1, fc1, null, null, null);
            sv1.setSourceLocation(sourceLoc);
            SelectSetOperation sop1 = new SelectSetOperation(new SetOperationInput(sb1, null), null);
            sop1.setSourceLocation(sourceLoc);
            SelectExpression se1 = new SelectExpression(null, sop1, null, null, true);
            se1.setSourceLocation(sourceLoc);

            inExpr = se1;
        }

        SqlCompatSelectCoercionKind cardinalityCoercion = ann.cardinalityCoercion;
        switch (cardinalityCoercion) {
            case SCALAR:
                /*
                 * inExpr = (SELECT ...)
                 * ->
                 * STRICT_FIRST_ELEMENT
                 * (
                 *  SELECT VALUE v2[(LEN(v2)-1)*2]
                 *  LET v2 = (SELECT VALUE v1 FROM (inExpr) v1 LIMIT 2)
                 * )
                 */

                /*
                 * E1: SELECT VALUE v1 FROM (inExpr) v1 LIMIT 2
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
            case NONE:
                // indicates that no transformation is necessary
                return inExpr;
            default:
                throw new CompilationException(ErrorCode.ILLEGAL_STATE, inExpr.getSourceLocation(), ann.toString());
        }
    }
}
