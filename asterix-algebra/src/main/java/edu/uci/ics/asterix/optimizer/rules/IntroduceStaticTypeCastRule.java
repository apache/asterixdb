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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Statically cast a constant from its type to a specified required type, in a
 * recursive way. It enables: 1. bag-based fields in a record, 2. bidirectional
 * cast of a open field and a matched closed field, and 3. put in null fields
 * when necessary. It should be fired before the constant folding rule.
 * 
 * This rule is not responsible for type casting between primitive types.
 * 
 * Here is an example: A record { "hobby": {{"music", "coding"}}, "id": "001",
 * "name": "Person Three"} which confirms to closed type ( id: string, name:
 * string, hobby: {{string}}? ) can be cast to an open type (id: string ), or
 * vice versa.
 * 
 * Implementation wise: first, we match the record's type and its target dataset
 * type to see if it is "cast-able"; second, if the types are cast-able, we
 * embed the required type into the original producer expression. If the types
 * are not cast-able, we throw a compile time exception.
 * 
 * Then, at runtime (not in this rule), the corresponding record/list
 * constructors know what to do by checking the required output type.
 * 
 * TODO: right now record/list constructor of the cast result is not done in the
 * ConstantFoldingRule and has to go to the runtime, because the
 * ConstantFoldingRule uses ARecordSerializerDeserializer which seems to have
 * some problem.
 */
public class IntroduceStaticTypeCastRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        /**
         * pattern match: sink/insert/assign record type is propagated from
         * insert data source to the record-constructor expression
         */
        if (context.checkIfInDontApplySet(this, opRef.getValue()))
            return false;
        context.addToDontApplySet(this, opRef.getValue());
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SINK)
            return false;
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE)
            return false;
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.ASSIGN)
            return false;

        /**
         * get required record type
         */
        InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) op2;
        AssignOperator topAssignOperator = (AssignOperator) op3;
        AqlDataSource dataSource = (AqlDataSource) insertDeleteOperator.getDataSource();
        IAType[] schemaTypes = (IAType[]) dataSource.getSchemaTypes();
        ARecordType requiredRecordType = (ARecordType) schemaTypes[schemaTypes.length - 1];

        /**
         * get input record type to the insert operator
         */
        List<LogicalVariable> usedVariables = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(topAssignOperator, usedVariables);

        // the used variable should contain the record that will be inserted
        // but it will not fail in many cases even if the used variable set is
        // empty
        if (usedVariables.size() == 0)
            return false;
        LogicalVariable oldRecordVariable = usedVariables.get(0);
        LogicalVariable inputRecordVar = usedVariables.get(0);
        IVariableTypeEnvironment env = topAssignOperator.computeOutputTypeEnvironment(context);
        ARecordType inputRecordType = (ARecordType) env.getVarType(inputRecordVar);

        AbstractLogicalOperator currentOperator = topAssignOperator;
        List<LogicalVariable> producedVariables = new ArrayList<LogicalVariable>();

        /**
         * find the assign operator for the "input record" to the insert_delete
         * operator
         */
        do {
            context.addToDontApplySet(this, currentOperator);
            if (currentOperator.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
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
                        ScalarFunctionCallExpression funcExpr = (ScalarFunctionCallExpression) expr;
                        // that expression has been rewritten, and it will not
                        // fail but just return false
                        if (TypeComputerUtilities.getRequiredType(funcExpr) != null)
                            return false;
                        IVariableTypeEnvironment assignEnv = topAssignOperator.computeOutputTypeEnvironment(context);
                        rewriteFuncExpr(funcExpr, requiredRecordType, inputRecordType, assignEnv);
                    }
                    context.computeAndSetTypeEnvironmentForOperator(originalAssign);
                }
            }
            if (currentOperator.getInputs().size() > 0)
                currentOperator = (AbstractLogicalOperator) currentOperator.getInputs().get(0).getValue();
            else
                break;
        } while (currentOperator != null);
        return true;
    }

    private void rewriteFuncExpr(ScalarFunctionCallExpression funcExpr, IAType reqType, IAType inputType,
            IVariableTypeEnvironment env) throws AlgebricksException {
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR) {
            rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType, env);
        } else if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR) {
            rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType, env);
        } else if (reqType.getTypeTag().equals(ATypeTag.RECORD)) {
            rewriteRecordFuncExpr(funcExpr, (ARecordType) reqType, (ARecordType) inputType, env);
        }
    }

    /**
     * only called when funcExpr is record constructor
     * 
     * @param funcExpr
     *            record constructor function expression
     * @param requiredListType
     *            required record type
     * @param inputRecordType
     * @param env
     *            type environment
     * @throws AlgebricksException
     */
    private void rewriteRecordFuncExpr(ScalarFunctionCallExpression funcExpr, ARecordType requiredRecordType,
            ARecordType inputRecordType, IVariableTypeEnvironment env) throws AlgebricksException {
        // if already rewritten, the required type is not null
        if (TypeComputerUtilities.getRequiredType(funcExpr) != null)
            return;
        TypeComputerUtilities.setRequiredAndInputTypes(funcExpr, requiredRecordType, inputRecordType);
        staticRecordTypeCast(funcExpr, requiredRecordType, inputRecordType, env);
    }

    /**
     * only called when funcExpr is list constructor
     * 
     * @param funcExpr
     *            list constructor function expression
     * @param requiredListType
     *            required list type
     * @param inputListType
     * @param env
     *            type environment
     * @throws AlgebricksException
     */
    private void rewriteListFuncExpr(ScalarFunctionCallExpression funcExpr, AbstractCollectionType requiredListType,
            AbstractCollectionType inputListType, IVariableTypeEnvironment env) throws AlgebricksException {
        if (TypeComputerUtilities.getRequiredType(funcExpr) != null)
            return;

        TypeComputerUtilities.setRequiredAndInputTypes(funcExpr, requiredListType, inputListType);
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();

        IAType itemType = requiredListType.getItemType();
        if (itemType == null || itemType.getTypeTag().equals(ATypeTag.ANY))
            return;
        IAType inputItemType = inputListType.getItemType();
        for (int j = 0; j < args.size(); j++) {
            ILogicalExpression arg = args.get(j).getValue();
            if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                ScalarFunctionCallExpression argFunc = (ScalarFunctionCallExpression) arg;
                IAType currentItemType = (IAType) env.getType(argFunc);
                if (inputItemType == null || inputItemType == BuiltinType.ANY) {
                    currentItemType = (IAType) env.getType(argFunc);
                    rewriteFuncExpr(argFunc, itemType, currentItemType, env);
                } else {
                    rewriteFuncExpr(argFunc, itemType, inputItemType, env);
                }
            }
        }
    }

    private void staticRecordTypeCast(ScalarFunctionCallExpression func, ARecordType reqType, ARecordType inputType,
            IVariableTypeEnvironment env) throws AlgebricksException {
        IAType[] reqFieldTypes = reqType.getFieldTypes();
        String[] reqFieldNames = reqType.getFieldNames();
        IAType[] inputFieldTypes = inputType.getFieldTypes();
        String[] inputFieldNames = inputType.getFieldNames();

        int[] fieldPermutation = new int[reqFieldTypes.length];
        boolean[] nullFields = new boolean[reqFieldTypes.length];
        boolean[] openFields = new boolean[inputFieldTypes.length];

        Arrays.fill(nullFields, false);
        Arrays.fill(openFields, true);
        Arrays.fill(fieldPermutation, -1);

        // forward match: match from actual to required
        boolean matched = false;
        for (int i = 0; i < inputFieldNames.length; i++) {
            String fieldName = inputFieldNames[i];
            IAType fieldType = inputFieldTypes[i];

            if (2 * i + 1 > func.getArguments().size())
                throw new AlgebricksException("expression index out of bound");

            // 2*i+1 is the index of field value expression
            ILogicalExpression arg = func.getArguments().get(2 * i + 1).getValue();
            matched = false;
            for (int j = 0; j < reqFieldNames.length; j++) {
                String reqFieldName = reqFieldNames[j];
                IAType reqFieldType = reqFieldTypes[j];
                if (fieldName.equals(reqFieldName)) {
                    if (fieldType.equals(reqFieldType)) {
                        fieldPermutation[j] = i;
                        openFields[i] = false;
                        matched = true;

                        if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            ScalarFunctionCallExpression scalarFunc = (ScalarFunctionCallExpression) arg;
                            rewriteFuncExpr(scalarFunc, reqFieldType, fieldType, env);
                        }
                        break;
                    }

                    // match the optional field
                    if (reqFieldType.getTypeTag() == ATypeTag.UNION
                            && NonTaggedFormatUtil.isOptionalField((AUnionType) reqFieldType)) {
                        IAType itemType = ((AUnionType) reqFieldType).getUnionList().get(
                                NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                        reqFieldType = itemType;
                        if (fieldType.equals(BuiltinType.ANULL) || fieldType.equals(itemType)) {
                            fieldPermutation[j] = i;
                            openFields[i] = false;
                            matched = true;

                            // rewrite record expr
                            if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                ScalarFunctionCallExpression scalarFunc = (ScalarFunctionCallExpression) arg;
                                rewriteFuncExpr(scalarFunc, reqFieldType, fieldType, env);
                            }
                            break;
                        }
                    }

                    // match the record field: need cast
                    if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        ScalarFunctionCallExpression scalarFunc = (ScalarFunctionCallExpression) arg;
                        rewriteFuncExpr(scalarFunc, reqFieldType, fieldType, env);
                        fieldPermutation[j] = i;
                        openFields[i] = false;
                        matched = true;
                        break;
                    }
                }
            }
            // the input has extra fields
            if (!matched && !reqType.isOpen())
                throw new AlgebricksException("static type mismatch: including an extra closed field " + fieldName);
        }

        // backward match: match from required to actual
        for (int i = 0; i < reqFieldNames.length; i++) {
            String reqFieldName = reqFieldNames[i];
            IAType reqFieldType = reqFieldTypes[i];
            matched = false;
            for (int j = 0; j < inputFieldNames.length; j++) {
                String fieldName = inputFieldNames[j];
                IAType fieldType = inputFieldTypes[j];
                if (!fieldName.equals(reqFieldName))
                    continue;
                // should check open field here
                // because number of entries in fieldPermuations is the
                // number of required schema fields
                // here we want to check if an input field is matched
                // the entry index of fieldPermuatons is req field index
                if (!openFields[j]) {
                    matched = true;
                    break;
                }

                // match the optional field
                if (reqFieldType.getTypeTag() == ATypeTag.UNION
                        && NonTaggedFormatUtil.isOptionalField((AUnionType) reqFieldType)) {
                    IAType itemType = ((AUnionType) reqFieldType).getUnionList().get(
                            NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                    if (fieldType.equals(BuiltinType.ANULL) || fieldType.equals(itemType)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (matched)
                continue;

            if (reqFieldType.getTypeTag() == ATypeTag.UNION
                    && NonTaggedFormatUtil.isOptionalField((AUnionType) reqFieldType)) {
                // add a null field
                nullFields[i] = true;
            } else {
                // no matched field in the input for a required closed field
                throw new AlgebricksException("static type mismatch: miss a required closed field " + reqFieldName);
            }
        }

        List<Mutable<ILogicalExpression>> arguments = func.getArguments();
        List<Mutable<ILogicalExpression>> originalArguments = new ArrayList<Mutable<ILogicalExpression>>();
        originalArguments.addAll(arguments);
        arguments.clear();
        // re-order the closed part and fill in null fields
        for (int i = 0; i < fieldPermutation.length; i++) {
            int pos = fieldPermutation[i];
            if (pos >= 0) {
                arguments.add(originalArguments.get(2 * pos));
                arguments.add(originalArguments.get(2 * pos + 1));
            }
            if (nullFields[i]) {
                // add a null field
                arguments.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                        new AString(reqFieldNames[i])))));
                arguments.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                        ANull.NULL))));
            }
        }

        // add the open part
        for (int i = 0; i < openFields.length; i++) {
            if (openFields[i]) {
                arguments.add(originalArguments.get(2 * i));
                Mutable<ILogicalExpression> fExprRef = originalArguments.get(2 * i + 1);
                ILogicalExpression argExpr = fExprRef.getValue();

                // we need to handle open fields recursively by their default
                // types
                // for list, their item type is any
                // for record, their
                if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    IAType reqFieldType = inputFieldTypes[i];
                    if (inputFieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                        reqFieldType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                    }
                    if (inputFieldTypes[i].getTypeTag() == ATypeTag.ORDEREDLIST) {
                        reqFieldType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                    }
                    if (inputFieldTypes[i].getTypeTag() == ATypeTag.UNORDEREDLIST) {
                        reqFieldType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
                    }
                    if (TypeComputerUtilities.getRequiredType((AbstractFunctionCallExpression) argExpr) == null) {
                        ScalarFunctionCallExpression argFunc = (ScalarFunctionCallExpression) argExpr;
                        rewriteFuncExpr(argFunc, reqFieldType, inputFieldTypes[i], env);
                    }
                }
                arguments.add(fExprRef);
            }
        }
    }
}
