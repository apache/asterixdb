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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Rewrites the exclusion list of a SELECT clause into a OBJECT_REMOVE_FIELDS function application. This rewrite
 * <b>MUST</b> run after {@link InlineColumnAliasVisitor}.
 * <p>
 * Input:
 * <pre>
 *   FROM     ...
 *   WHERE    ...
 *   SELECT   * EXCLUDE a, b.f
 *   ORDER BY c
 * </pre>
 * Output:
 * <pre>
 *   FROM     ( FROM   ...
 *              WHERE  ...
 *              SELECT *
 *              ORDER BY c ) TMP_1
 *   SELECT   VALUE OBJECT_REMOVE_FIELDS(TMP_1, [ "a", [ "b", "f" ]])
 * </pre>
 * <p>
 * There exists a special case with a single {@link FromTerm} node (with no other local bindings) and a SELECT * clause,
 * where we qualify our field exclusion list with the {@link FromTerm} variable if we cannot anchor on the dataset
 * variable. For example:
 * <pre>
 *   FROM   MyDataset D
 *   SELECT * EXCLUDE a, b.c, D.d
 * </pre>
 * Is conceptually processed as:
 * <pre>
 *   FROM   MyDataset D
 *   SELECT * EXCLUDE D.a, D.b.c, D.d
 * </pre>
 * For all other cases, our EXCLUDE will work solely with what our SELECT returns.
 */
public class SelectExcludeRewriteSugarVisitor extends AbstractSqlppExpressionScopingVisitor {
    public SelectExcludeRewriteSugarVisitor(LangRewritingContext langRewritingContext) {
        super(langRewritingContext);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        super.visit(selectBlock, arg);

        // Proceed if we have field-exclusions.
        SelectClause selectClause = selectBlock.getSelectClause();
        if (selectClause.getFieldExclusions().isEmpty()) {
            return null;
        }
        SelectExpression selectExpression = (SelectExpression) arg;

        // If we have a single dataset in a FROM-CLAUSE (with no other variables in our local scope / grouping)...
        if (selectBlock.hasFromClause() && selectBlock.getFromClause().getFromTerms().size() == 1) {
            FromTerm fromTerm = selectBlock.getFromClause().getFromTerms().get(0);
            if (!selectBlock.hasGroupbyClause() && !fromTerm.hasCorrelateClauses() && selectBlock.getLetWhereList()
                    .stream().noneMatch(c -> c.getClauseType() == Clause.ClauseType.LET_CLAUSE)) {
                // ...and we have a 'SELECT *'...
                SelectRegular selectRegular = selectClause.getSelectRegular();
                if (selectClause.selectRegular() && selectRegular.getProjections().size() == 1
                        && selectRegular.getProjections().get(0).getKind() == Projection.Kind.STAR) {
                    // ...then qualify our field exclusions with our FROM-CLAUSE variable.
                    String fromTermName = fromTerm.getLeftVariable().getVar().getValue();
                    String qualifier = SqlppVariableUtil.toUserDefinedName(fromTermName);
                    selectClause.getFieldExclusions().stream().filter(e -> {
                        // Do not needlessly qualify names that are already bound to variables in our scope.
                        // Note: We use our local scope to include the single-dataset variable AND our outer scope.
                        //       We already know that there are no other variables in our local scope.
                        Iterator<Pair<Identifier, Set<? extends Scope.SymbolAnnotation>>> liveSymbolIterator =
                                scopeChecker.getCurrentScope().liveSymbols(null);
                        while (liveSymbolIterator.hasNext()) {
                            Pair<Identifier, Set<? extends Scope.SymbolAnnotation>> symbol = liveSymbolIterator.next();
                            String symbolName = SqlppVariableUtil.toUserDefinedName(symbol.first.getValue());
                            if (symbolName.equals(e.get(0))) {
                                return false;
                            }
                        }
                        return true;
                    }).forEach(e -> e.add(0, qualifier));
                }
            }
        }

        // Find our parent SET-OP-INPUT.
        SetOperationInput setOperationInput = null;
        SelectSetOperation selectSetOperation = selectExpression.getSelectSetOperation();
        if (selectBlock.equals(selectSetOperation.getLeftInput().getSelectBlock())) {
            setOperationInput = selectSetOperation.getLeftInput();
        } else {
            for (SetOperationRight rightInput : selectSetOperation.getRightInputs()) {
                SetOperationInput setOperationRightInput = rightInput.getSetOperationRightInput();
                if (selectBlock.equals(setOperationRightInput.getSelectBlock())) {
                    setOperationInput = setOperationRightInput;
                    break;
                }
            }
        }
        if (setOperationInput == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectBlock.getSourceLocation(),
                    "Parent SET-OP-INPUT not found while rewriting SELECT-EXCLUDE!");
        }

        // Nest our original SELECT-BLOCK.
        SourceLocation sourceLocation = selectBlock.getSourceLocation();
        SetOperationInput innerSetOpInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation innerSelectSetOp = new SelectSetOperation(innerSetOpInput, null);
        innerSelectSetOp.setSourceLocation(sourceLocation);
        SelectExpression innerSelectExpr;
        if (!selectSetOperation.hasRightInputs()) {
            // We need to attach our LET / ORDER BY / LIMIT to our inner SELECT-EXPR.
            SelectExpression selectExprCopy = (SelectExpression) SqlppRewriteUtil.deepCopy(selectExpression);
            innerSelectExpr = new SelectExpression(selectExprCopy.getLetList(), innerSelectSetOp,
                    selectExprCopy.getOrderbyClause(), selectExprCopy.getLimitClause(), true);
            selectExpression.getLetList().clear();
            selectExpression.setOrderbyClause(null);
            selectExpression.setLimitClause(null);
        } else {
            innerSelectExpr = new SelectExpression(null, innerSelectSetOp, null, null, true);
        }
        innerSelectExpr.setSourceLocation(sourceLocation);

        // Build a new SELECT-BLOCK.
        VarIdentifier fromTermVariable = context.newVariable();
        VariableExpr fromTermVariableExpr = new VariableExpr(fromTermVariable);
        SelectClause innerSelectClause = buildSelectClause(selectClause, fromTermVariable);
        innerSelectClause.setSourceLocation(sourceLocation);
        FromTerm innerFromTerm = new FromTerm(innerSelectExpr, fromTermVariableExpr, null, null);
        innerFromTerm.setSourceLocation(sourceLocation);
        FromClause innerFromClause = new FromClause(List.of(innerFromTerm));
        innerFromClause.setSourceLocation(sourceLocation);
        SelectBlock innerSelectBlock = new SelectBlock(innerSelectClause, innerFromClause, null, null, null);
        setOperationInput.setSelectBlock(innerSelectBlock);
        return null;
    }

    private SelectClause buildSelectClause(SelectClause originalSelectClause, VarIdentifier iterationVariable) {
        // Convert our list of identifiers into a list of literals representing field names.
        ListConstructor listConstructor = new ListConstructor();
        listConstructor.setType(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR);
        listConstructor.setExprList(new ArrayList<>());
        for (List<String> nestedField : originalSelectClause.getFieldExclusions()) {
            if (nestedField.size() == 1) {
                // For non-nested fields, we do not wrap our name in a list.
                listConstructor.getExprList().add(new LiteralExpr(new StringLiteral(nestedField.get(0))));
            } else {
                // Otherwise, build a list to insert into our list.
                ListConstructor nestedFieldList = new ListConstructor();
                nestedFieldList.setType(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR);
                nestedFieldList.setExprList(nestedField.stream().map(f -> new LiteralExpr(new StringLiteral(f)))
                        .collect(Collectors.toList()));
                listConstructor.getExprList().add(nestedFieldList);
            }
        }
        List<Expression> objectRemoveFieldsArguments = new ArrayList<>();
        objectRemoveFieldsArguments.add(new VariableExpr(iterationVariable));
        objectRemoveFieldsArguments.add(listConstructor);
        originalSelectClause.getFieldExclusions().clear();

        // Remove the DISTINCT from our original SELECT-CLAUSE, if it exists.
        boolean isDistinct = originalSelectClause.distinct();
        if (isDistinct) {
            originalSelectClause.setDistinct(false);
        }

        // Create the call to OBJECT_REMOVE_FIELDS.
        FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.REMOVE_FIELDS);
        CallExpr callExpr = new CallExpr(functionSignature, objectRemoveFieldsArguments);
        SelectElement selectElement = new SelectElement(callExpr);
        SelectClause selectClause = new SelectClause(selectElement, null, isDistinct);
        selectClause.setSourceLocation(originalSelectClause.getSourceLocation());
        return selectClause;
    }
}
