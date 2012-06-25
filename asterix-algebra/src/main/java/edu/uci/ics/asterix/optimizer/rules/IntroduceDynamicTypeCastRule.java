/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Dynamically cast a variable from its type to a specified required type, in a
 * recursive way. It enables: 1. bag-based fields in a record, 2. bidirectional
 * cast of a open field and a matched closed field, and 3. put in null fields
 * when necessary.
 * 
 * Here is an example: A record { "hobby": {{"music", "coding"}}, "id": "001",
 * "name": "Person Three"} which confirms to closed type ( id: string, name:
 * string, hobby: {{string}}? ) can be cast to an open type (id: string ), or
 * vice versa.
 * 
 * However, if the input record is a variable, then we don't know its exact
 * field layout at compile time. For example, records conforming to the same
 * type can have different field orderings and different open parts. That's why
 * we need dynamic type casting.
 * 
 * Note that as we can see in the example, the ordering of fields of a record is
 * not required. Since the open/closed part of a record has completely different
 * underlying memory/storage layout, a cast-record function will change the
 * layout as specified at runtime.
 * 
 * Implementation wise, this rule checks the target dataset type and the input
 * record type, and if the types are different, then it plugs in an assign with
 * a cast-record function, and projects away the original (uncast) field.
 */
public class IntroduceDynamicTypeCastRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        /**
         * pattern match: sink insert assign
         * 
         * resulting plan: sink-insert-project-assign
         * 
         */
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SINK)
            return false;
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE)
            return false;
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.ASSIGN)
            return false;

        InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) op2;
        AssignOperator oldAssignOperator = (AssignOperator) op3;

        AqlDataSource dataSource = (AqlDataSource) insertDeleteOperator.getDataSource();
        IAType[] schemaTypes = (IAType[]) dataSource.getSchemaTypes();
        ARecordType requiredRecordType = (ARecordType) schemaTypes[schemaTypes.length - 1];

        List<LogicalVariable> usedVariables = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(oldAssignOperator, usedVariables);
        LogicalVariable inputRecordVar;
        if (usedVariables.size() > 0) {
            inputRecordVar = usedVariables.get(0);
        } else {
            VariableUtilities.getLiveVariables(oldAssignOperator, usedVariables);
            inputRecordVar = usedVariables.get(0);
        }
        IVariableTypeEnvironment env = oldAssignOperator.computeInputTypeEnvironment(context);
        ARecordType inputRecordType = (ARecordType) env.getVarType(inputRecordVar);

        boolean needCast = !requiredRecordType.equals(inputRecordType);
        if (!needCast)
            return false;

        // insert
        // project
        // assign
        // assign
        AbstractFunctionCallExpression cast = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CAST_RECORD));
        ARecordType[] types = new ARecordType[2];
        types[0] = requiredRecordType;
        types[1] = inputRecordType;
        cast.getArguments().add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(inputRecordVar)));
        cast.setOpaqueParameters(types);
        LogicalVariable newAssignVar = context.newVar();
        AssignOperator newAssignOperator = new AssignOperator(newAssignVar, new MutableObject<ILogicalExpression>(cast));
        newAssignOperator.getInputs().add(new MutableObject<ILogicalOperator>(op3));

        List<LogicalVariable> projectVariables = new ArrayList<LogicalVariable>();
        VariableUtilities.getProducedVariables(oldAssignOperator, projectVariables);
        projectVariables.add(newAssignVar);
        ProjectOperator projectOperator = new ProjectOperator(projectVariables);
        projectOperator.getInputs().add(new MutableObject<ILogicalOperator>(newAssignOperator));

        ILogicalExpression payloadExpr = new VariableReferenceExpression(newAssignVar);
        MutableObject<ILogicalExpression> payloadRef = new MutableObject<ILogicalExpression>(payloadExpr);
        InsertDeleteOperator newInserDeleteOperator = new InsertDeleteOperator(insertDeleteOperator.getDataSource(),
                payloadRef, insertDeleteOperator.getPrimaryKeyExpressions(), insertDeleteOperator.getOperation());
        newInserDeleteOperator.getInputs().add(new MutableObject<ILogicalOperator>(projectOperator));
        insertDeleteOperator.getInputs().clear();
        op1.getInputs().get(0).setValue(newInserDeleteOperator);
        return true;
    }

}
