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
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Dynamically cast a variable from its type to a specified required type, in a
 * recursive way. It enables: 1. bag-based fields in a record, 2. bidirectional
 * cast of a open field and a matched closed field, and 3. put in null fields
 * when necessary.
 * Here is an example: A record { "hobby": {{"music", "coding"}}, "id": "001",
 * "name": "Person Three"} which confirms to closed type ( id: string, name:
 * string, hobby: {{string}}? ) can be cast to an open type (id: string ), or
 * vice versa.
 * However, if the input record is a variable, then we don't know its exact
 * field layout at compile time. For example, records conforming to the same
 * type can have different field orderings and different open parts. That's why
 * we need dynamic type casting.
 * Note that as we can see in the example, the ordering of fields of a record is
 * not required. Since the open/closed part of a record has completely different
 * underlying memory/storage layout, a cast-record function will change the
 * layout as specified at runtime.
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
         * resulting plan: sink-insert-project-assign
         */
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SINK)
            return false;
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE)
            return false;
        InsertDeleteOperator insertDeleteOp = (InsertDeleteOperator) op2;
        if (insertDeleteOp.getOperation() == InsertDeleteOperator.Kind.DELETE)
            return false;

        InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) op2;
        AqlDataSource dataSource = (AqlDataSource) insertDeleteOperator.getDataSource();
        IAType[] schemaTypes = (IAType[]) dataSource.getSchemaTypes();
        ARecordType requiredRecordType = (ARecordType) schemaTypes[schemaTypes.length - 1];
        ILogicalExpression expr = insertDeleteOperator.getPayloadExpression().getValue();
        List<LogicalVariable> payloadVars = new ArrayList<LogicalVariable>();
        expr.getUsedVariables(payloadVars);
        LogicalVariable recordVar = payloadVars.get(0);
        IVariableTypeEnvironment env = insertDeleteOperator.computeOutputTypeEnvironment(context);
        IAType inputRecordType = (IAType) env.getVarType(recordVar);
        if (capatible(requiredRecordType, inputRecordType)) {
            return false;
        }

        LogicalVariable replacedVar = addCast(requiredRecordType, recordVar, insertDeleteOp, context);
        return replacedVar == null;
    }

    /**
     * Inject a cast-record function when necessary
     * 
     * @param requiredRecordType
     *            the required record type
     * @param recordVar
     *            the record variable
     * @param parent
     *            the current parent operator to be rewritten
     * @param context
     *            the optimization context
     * @return true if cast is injected; false otherwise.
     * @throws AlgebricksException
     */
    public LogicalVariable addCast(ARecordType requiredRecordType, LogicalVariable recordVar, ILogicalOperator parent,
            IOptimizationContext context) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> opRefs = parent.getInputs();
        for (int index = 0; index < opRefs.size(); index++) {
            Mutable<ILogicalOperator> opRef = opRefs.get(index);
            ILogicalOperator op = opRef.getValue();

            List<LogicalVariable> producedVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getProducedVariables(op, producedVars);
            IVariableTypeEnvironment env = op.computeOutputTypeEnvironment(context);

            for (int i = 0; i < producedVars.size(); i++) {
                LogicalVariable var = producedVars.get(i);
                if (var.equals(recordVar)) {
                    IAType actualType = (IAType) env.getVarType(var);
                    if (!capatible(requiredRecordType, actualType)) {
                        AbstractFunctionCallExpression cast = new ScalarFunctionCallExpression(
                                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CAST_RECORD));
                        cast.getArguments().add(
                                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
                        TypeComputerUtilities.setRequiredAndInputTypes(cast, requiredRecordType, actualType);
                        LogicalVariable newAssignVar = context.newVar();
                        AssignOperator newAssignOperator = new AssignOperator(newAssignVar,
                                new MutableObject<ILogicalExpression>(cast));
                        newAssignOperator.getInputs().add(new MutableObject<ILogicalOperator>(op));
                        opRef.setValue(newAssignOperator);
                        context.computeAndSetTypeEnvironmentForOperator(parent);
                        context.computeAndSetTypeEnvironmentForOperator(newAssignOperator);
                        newAssignOperator.computeOutputTypeEnvironment(context);
                        VariableUtilities.substituteVariables(parent, recordVar, newAssignVar, context);
                        return newAssignVar;
                    }
                }
            }
            LogicalVariable replacedVar = addCast(requiredRecordType, recordVar, op, context);
            if (replacedVar != null) {
                VariableUtilities.substituteVariables(parent, recordVar, replacedVar, context);
                return replacedVar;
            }
        }
        return null;
    }

    private boolean capatible(ARecordType reqType, IAType inputType) {
        if (inputType.getTypeTag() == ATypeTag.ANY) {
            return false;
        }
        IAType[] reqTypes = reqType.getFieldTypes();
        String[] reqFieldNames = reqType.getFieldNames();
        IAType[] inputTypes = ((ARecordType) inputType).getFieldTypes();
        String[] inputFieldNames = ((ARecordType) inputType).getFieldNames();

        if (reqTypes.length != inputTypes.length) {
            return false;
        }
        for (int i = 0; i < reqTypes.length; i++) {
            if (!reqFieldNames[i].equals(inputFieldNames[i])) {
                return false;
            }
            IAType reqTypeInside = reqTypes[i];
            if (reqTypes[i].getTypeTag() == ATypeTag.UNION
                    && NonTaggedFormatUtil.isOptionalField((AUnionType) reqTypes[i])) {
                reqTypeInside = ((AUnionType) reqTypes[i]).getUnionList().get(
                        NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
            }
            IAType inputTypeInside = inputTypes[i];
            if (inputTypes[i].getTypeTag() == ATypeTag.UNION
                    && NonTaggedFormatUtil.isOptionalField((AUnionType) inputTypes[i])) {
                inputTypeInside = ((AUnionType) inputTypes[i]).getUnionList().get(
                        NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
            }
            if (inputTypeInside.getTypeTag() != ATypeTag.NULL && !reqTypeInside.equals(inputTypeInside)) {
                return false;
            }
        }
        return true;
    }
}
