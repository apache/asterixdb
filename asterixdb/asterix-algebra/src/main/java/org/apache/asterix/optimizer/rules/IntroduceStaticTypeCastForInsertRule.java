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

package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.typecast.StaticTypeCastUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Statically cast a constant from its type to a specified required type, in a
 * recursive way. It enables: 1. bag-based fields in a record, 2. bidirectional
 * cast of an open field and a matched closed field, and 3. put in null fields
 * when necessary. It should be fired before the constant folding rule.
 * This rule is not responsible for type casting between primitive types.
 * Here is an example: A record { "hobby": {{"music", "coding"}}, "id": "001",
 * "name": "Person Three"} which confirms to closed type ( id: string, name:
 * string, hobby: {{string}}? ) can be cast to an open type (id: string ), or
 * vice versa.
 * Implementation wise: first, we match the record's type and its target dataset
 * type to see if it is "cast-able"; second, if the types are cast-able, we
 * embed the required type into the original producer expression. If the types
 * are not cast-able, we throw a compile time exception.
 * Then, at runtime (not in this rule), the corresponding record/list
 * constructors know what to do by checking the required output type.
 * TODO: right now record/list constructor of the cast result is not done in the
 * ConstantFoldingRule and has to go to the runtime, because the
 * ConstantFoldingRule uses ARecordSerializerDeserializer which seems to have
 * some problem.
 */
public class IntroduceStaticTypeCastForInsertRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        /**
         * pattern match: sink/insert/assign record type is propagated from
         * insert data source to the record-constructor expression
         */
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        context.addToDontApplySet(this, opRef.getValue());

        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        List<LogicalVariable> producedVariables = new ArrayList<LogicalVariable>();
        LogicalVariable oldRecordVariable;

        if (op1.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR
                && op1.getOperatorTag() != LogicalOperatorTag.SINK) {
            return false;
        }
        if (op1.getOperatorTag() == LogicalOperatorTag.DELEGATE_OPERATOR) {
            DelegateOperator eOp = (DelegateOperator) op1;
            if (!(eOp.getDelegate() instanceof CommitOperator)) {
                return false;
            }
        }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE_UPSERT) {
            return false;
        }
        InsertDeleteUpsertOperator insertDeleteOp = (InsertDeleteUpsertOperator) op2;
        if (insertDeleteOp.getOperation() == InsertDeleteUpsertOperator.Kind.DELETE) {
            return false;
        }
        /**
         * get required record type
         */
        InsertDeleteUpsertOperator insertDeleteOperator = (InsertDeleteUpsertOperator) op2;
        DataSource dataSource = (DataSource) insertDeleteOperator.getDataSource();
        IAType requiredRecordType = dataSource.getItemType();

        List<LogicalVariable> usedVariables = new ArrayList<LogicalVariable>();
        insertDeleteOperator.getPayloadExpression().getValue().getUsedVariables(usedVariables);

        // the used variable should contain the record that will be inserted
        // but it will not fail in many cases even if the used variable set is
        // empty
        if (usedVariables.size() == 0) {
            return false;
        }

        oldRecordVariable = usedVariables.get(0);
        LogicalVariable inputRecordVar = usedVariables.get(0);
        IVariableTypeEnvironment env = insertDeleteOperator.computeOutputTypeEnvironment(context);
        IAType inputRecordType = (IAType) env.getVarType(inputRecordVar);

        AbstractLogicalOperator currentOperator = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
        /**
         * find the assign operator for the "input record" to the insert_delete
         * operator
         */
        do {
            context.addToDontApplySet(this, currentOperator);
            if (currentOperator.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) currentOperator;
                producedVariables.clear();
                VariableUtilities.getProducedVariables(currentOperator, producedVariables);
                int position = producedVariables.indexOf(oldRecordVariable);

                /**
                 * set the top-down propagated type
                 */
                if (position >= 0) {
                    AssignOperator originalAssign = (AssignOperator) currentOperator;
                    List<Mutable<ILogicalExpression>> expressionRefs = originalAssign.getExpressions();
                    ILogicalExpression expr = expressionRefs.get(position).getValue();
                    if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                        // that expression has been rewritten, and it will not
                        // fail but just return false
                        if (TypeCastUtils.getRequiredType(funcExpr) != null) {
                            context.computeAndSetTypeEnvironmentForOperator(assignOp);
                            return false;
                        }
                        IVariableTypeEnvironment assignEnv = assignOp.computeOutputTypeEnvironment(context);
                        StaticTypeCastUtil.rewriteFuncExpr(funcExpr, requiredRecordType, inputRecordType, assignEnv);
                    }
                    context.computeAndSetTypeEnvironmentForOperator(originalAssign);
                }
            }
            if (currentOperator.getInputs().size() > 0) {
                currentOperator = (AbstractLogicalOperator) currentOperator.getInputs().get(0).getValue();
            } else {
                break;
            }
        } while (currentOperator != null);
        return true;
    }

}
