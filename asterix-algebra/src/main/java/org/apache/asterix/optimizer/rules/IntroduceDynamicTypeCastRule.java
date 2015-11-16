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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.asterix.lang.aql.util.FunctionUtils;
import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeComputerUtilities;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Dynamically cast a variable from its type to a specified required type, in a
 * recursive way. It enables: 1. bag-based fields in a record, 2. bidirectional
 * cast of a open field and a matched closed field, and 3. put in null fields
 * when necessary.
 * Here is an example: A record { "hobby": {{"music", "coding"}}, "id": "001",
 * "name": "Person Three"} which conforms to closed type ( id: string, name:
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
        // Depending on the operator type, we need to extract the following pieces of information.
        AbstractLogicalOperator op;
        ARecordType requiredRecordType;
        LogicalVariable recordVar;

        // We identify INSERT and DISTRIBUTE_RESULT operators.
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        switch (op1.getOperatorTag()) {
            case SINK: {
                /**
                 * pattern match: sink insert assign
                 * resulting plan: sink-insert-project-assign
                 */

                AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
                if (op2.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE) {
                    InsertDeleteOperator insertDeleteOp = (InsertDeleteOperator) op2;
                    if (insertDeleteOp.getOperation() == InsertDeleteOperator.Kind.DELETE)
                        return false;

                    // Remember this is the operator we need to modify
                    op = insertDeleteOp;

                    // Derive the required ARecordType based on the schema of the AqlDataSource
                    InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) op2;
                    AqlDataSource dataSource = (AqlDataSource) insertDeleteOperator.getDataSource();
                    IAType[] schemaTypes = (IAType[]) dataSource.getSchemaTypes();
                    requiredRecordType = (ARecordType) schemaTypes[schemaTypes.length - 1];

                    // Derive the Variable which we will potentially wrap with cast/null functions
                    ILogicalExpression expr = insertDeleteOperator.getPayloadExpression().getValue();
                    List<LogicalVariable> payloadVars = new ArrayList<LogicalVariable>();
                    expr.getUsedVariables(payloadVars);
                    recordVar = payloadVars.get(0);
                } else {
                    return false;
                }

                break;
            }
            case DISTRIBUTE_RESULT: {
                // First, see if there was an output-record-type specified
                requiredRecordType = (ARecordType) op1.getAnnotations().get("output-record-type");
                if (requiredRecordType == null) {
                    return false;
                }

                // Remember this is the operator we need to modify
                op = op1;

                // The Variable we want is the (hopefully singular, hopefully record-typed) live variable
                // of the singular input operator of the DISTRIBUTE_RESULT
                if (op.getInputs().size() > 1) {
                    // Hopefully not possible?
                    throw new AlgebricksException(
                            "output-record-type defined for expression with multiple input operators");
                }
                AbstractLogicalOperator input = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
                List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
                VariableUtilities.getLiveVariables(input, liveVars);
                if (liveVars.size() > 1) {
                    throw new AlgebricksException(
                            "Expression with multiple fields cannot be cast to output-record-type!");
                }
                recordVar = liveVars.get(0);

                break;
            }
            default: {
                return false;
            }
        }

        // Derive the statically-computed type of the record
        IVariableTypeEnvironment env = op.computeOutputTypeEnvironment(context);
        IAType inputRecordType = (IAType) env.getVarType(recordVar);

        /** the input record type can be an union type -- for the case when it comes from a subplan or left-outer join */
        boolean checkNull = false;
        while (NonTaggedFormatUtil.isOptional(inputRecordType)) {
            /** while-loop for the case there is a nested multi-level union */
            inputRecordType = ((AUnionType) inputRecordType).getNullableType();
            checkNull = true;
        }

        /** see whether the input record type needs to be casted */
        boolean cast = !compatible(requiredRecordType, inputRecordType);

        if (checkNull) {
            recordVar = addWrapperFunction(requiredRecordType, recordVar, op, context, AsterixBuiltinFunctions.NOT_NULL);
        }
        if (cast) {
            addWrapperFunction(requiredRecordType, recordVar, op, context, AsterixBuiltinFunctions.CAST_RECORD);
        }
        return cast || checkNull;
    }

    /**
     * Inject a function to wrap a variable when necessary
     *
     * @param requiredRecordType
     *            the required record type
     * @param recordVar
     *            the record variable
     * @param parent
     *            the current parent operator to be rewritten
     * @param context
     *            the optimization context
     * @param fd
     *            the function to be injected
     * @return true if cast is injected; false otherwise.
     * @throws AlgebricksException
     */
    public static LogicalVariable addWrapperFunction(ARecordType requiredRecordType, LogicalVariable recordVar,
            ILogicalOperator parent, IOptimizationContext context, FunctionIdentifier fd) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> opRefs = parent.getInputs();
        for (int index = 0; index < opRefs.size(); index++) {
            Mutable<ILogicalOperator> opRef = opRefs.get(index);
            ILogicalOperator op = opRef.getValue();

            /** get produced vars */
            List<LogicalVariable> producedVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getProducedVariables(op, producedVars);
            IVariableTypeEnvironment env = op.computeOutputTypeEnvironment(context);
            for (int i = 0; i < producedVars.size(); i++) {
                LogicalVariable var = producedVars.get(i);
                if (var.equals(recordVar)) {
                    /** insert an assign operator to call the function on-top-of the variable */
                    IAType actualType = (IAType) env.getVarType(var);
                    AbstractFunctionCallExpression cast = new ScalarFunctionCallExpression(
                            FunctionUtils.getFunctionInfo(fd));
                    cast.getArguments()
                            .add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
                    /** enforce the required record type */
                    TypeComputerUtilities.setRequiredAndInputTypes(cast, requiredRecordType, actualType);
                    LogicalVariable newAssignVar = context.newVar();
                    AssignOperator newAssignOperator = new AssignOperator(newAssignVar,
                            new MutableObject<ILogicalExpression>(cast));
                    newAssignOperator.getInputs().add(new MutableObject<ILogicalOperator>(op));
                    opRef.setValue(newAssignOperator);
                    context.computeAndSetTypeEnvironmentForOperator(newAssignOperator);
                    newAssignOperator.computeOutputTypeEnvironment(context);
                    VariableUtilities.substituteVariables(parent, recordVar, newAssignVar, context);
                    return newAssignVar;
                }
            }
            /** recursive descend to the operator who produced the recordVar */
            LogicalVariable replacedVar = addWrapperFunction(requiredRecordType, recordVar, op, context, fd);
            if (replacedVar != null) {
                /** substitute the recordVar by the replacedVar for operators who uses recordVar */
                VariableUtilities.substituteVariables(parent, recordVar, replacedVar, context);
                return replacedVar;
            }
        }
        return null;
    }

    /**
     * Check whether the required record type and the input type is compatible
     *
     * @param reqType
     * @param inputType
     * @return true if compatible; false otherwise
     * @throws AlgebricksException
     */
    public static boolean compatible(ARecordType reqType, IAType inputType) throws AlgebricksException {
        if (inputType.getTypeTag() == ATypeTag.ANY) {
            return false;
        }
        if (inputType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("The input type " + inputType + " is not a valid record type!");
        }
        ARecordType inputRecType = (ARecordType) inputType;
        if (reqType.isOpen() != inputRecType.isOpen()) {
            return false;
        }

        IAType[] reqTypes = reqType.getFieldTypes();
        String[] reqFieldNames = reqType.getFieldNames();
        IAType[] inputTypes = inputRecType.getFieldTypes();
        String[] inputFieldNames = ((ARecordType) inputType).getFieldNames();

        if (reqTypes.length != inputTypes.length) {
            return false;
        }
        for (int i = 0; i < reqTypes.length; i++) {
            if (!reqFieldNames[i].equals(inputFieldNames[i])) {
                return false;
            }
            IAType reqTypeInside = reqTypes[i];
            if (NonTaggedFormatUtil.isOptional(reqTypes[i])) {
                reqTypeInside = ((AUnionType) reqTypes[i]).getNullableType();
            }
            IAType inputTypeInside = inputTypes[i];
            if (NonTaggedFormatUtil.isOptional(inputTypes[i])) {
                if (!NonTaggedFormatUtil.isOptional(reqTypes[i])) {
                    /** if the required type is not optional, the two types are incompatible */
                    return false;
                }
                inputTypeInside = ((AUnionType) inputTypes[i]).getNullableType();
            }
            if (inputTypeInside.getTypeTag() != ATypeTag.NULL && !reqTypeInside.equals(inputTypeInside)) {
                return false;
            }
        }
        return true;
    }
}
