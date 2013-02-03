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

package edu.uci.ics.asterix.optimizer.rules.typecast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.AsterixFunctionInfo;
import edu.uci.ics.asterix.om.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;

/**
 * This class is utility to do type cast.
 * It offers two public methods:
 * 1. public static boolean rewriteListExpr(AbstractFunctionCallExpression funcExpr, IAType reqType, IAType inputType,
 * IVariableTypeEnvironment env) throws AlgebricksException, which only enforces the list type recursively.
 * 2. public static boolean rewriteFuncExpr(AbstractFunctionCallExpression funcExpr, IAType reqType, IAType inputType,
 * IVariableTypeEnvironment env) throws AlgebricksException, which enforces the list type and the record type recursively.
 * 
 * @author yingyib
 */
public class StaticTypeCastUtil {

    /**
     * This method is only called when funcExpr contains list constructor function calls.
     * The List constructor is very special because a nested list is of type List<ANY>.
     * However, the bottom-up type inference (InferTypeRule in algebricks) did not infer that so we need this method to enforce the type.
     * We do not want to break the generality of algebricks so this method is called in an ASTERIX rule: @ IntroduceEnforcedListTypeRule} .
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
    public static boolean rewriteListExpr(AbstractFunctionCallExpression funcExpr, IAType reqType, IAType inputType,
            IVariableTypeEnvironment env) throws AlgebricksException {
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR) {
            if (reqType.equals(BuiltinType.ANY)) {
                reqType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
            }
            return rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType,
                    env);
        } else if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR) {
            if (reqType.equals(BuiltinType.ANY)) {
                reqType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            return rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType,
                    env);
        } else {
            List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
            boolean changed = false;
            for (Mutable<ILogicalExpression> arg : args) {
                ILogicalExpression argExpr = arg.getValue();
                if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
                    IAType exprType = (IAType) env.getType(argFuncExpr);
                    changed = changed || rewriteListExpr(argFuncExpr, exprType, exprType, env);
                }
            }
            return changed;
        }
    }

    /**
     * This method is to recursively enforce required types, for the list type and the record type.
     * The List constructor is very special because
     * 1. a nested list in a list is of type List<ANY>;
     * 2. a nested record in a list is of type Open_Record{}.
     * The open record constructor is very special because
     * 1. a nested list in the open part is of type List<ANY>;
     * 2. a nested record in the open part is of type Open_Record{}.
     * However, the bottom-up type inference (InferTypeRule in algebricks) did not infer that so we need this method to enforce the type.
     * We do not want to break the generality of algebricks so this method is called in an ASTERIX rule: @ IntroduceStaticTypeCastRule} .
     * 
     * @param funcExpr
     *            the function expression whose type needs to be top-down enforced
     * @param reqType
     *            the required type inferred from parent operators/expressions
     * @param inputType
     *            the current inferred
     * @param env
     *            the type environment
     * @return true if the type is casted; otherwise, false.
     * @throws AlgebricksException
     */
    public static boolean rewriteFuncExpr(AbstractFunctionCallExpression funcExpr, IAType reqType, IAType inputType,
            IVariableTypeEnvironment env) throws AlgebricksException {
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR) {
            if (reqType.equals(BuiltinType.ANY)) {
                reqType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
            }
            return rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType,
                    env);
        } else if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR) {
            if (reqType.equals(BuiltinType.ANY)) {
                reqType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            return rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType,
                    env);
        } else if (inputType.getTypeTag().equals(ATypeTag.RECORD)) {
            if (reqType.equals(BuiltinType.ANY)) {
                reqType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            return rewriteRecordFuncExpr(funcExpr, (ARecordType) reqType, (ARecordType) inputType, env);
        } else {
            List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
            boolean changed = false;
            for (Mutable<ILogicalExpression> arg : args) {
                ILogicalExpression argExpr = arg.getValue();
                if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
                    IAType exprType = (IAType) env.getType(argFuncExpr);
                    changed = changed || rewriteFuncExpr(argFuncExpr, exprType, exprType, env);
                }
            }
            return changed;
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
    private static boolean rewriteRecordFuncExpr(AbstractFunctionCallExpression funcExpr,
            ARecordType requiredRecordType, ARecordType inputRecordType, IVariableTypeEnvironment env)
            throws AlgebricksException {
        // if already rewritten, the required type is not null
        if (TypeComputerUtilities.getRequiredType(funcExpr) != null)
            return false;
        TypeComputerUtilities.setRequiredAndInputTypes(funcExpr, requiredRecordType, inputRecordType);
        staticRecordTypeCast(funcExpr, requiredRecordType, inputRecordType, env);
        return true;
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
    private static boolean rewriteListFuncExpr(AbstractFunctionCallExpression funcExpr,
            AbstractCollectionType requiredListType, AbstractCollectionType inputListType, IVariableTypeEnvironment env)
            throws AlgebricksException {
        if (TypeComputerUtilities.getRequiredType(funcExpr) != null)
            return false;

        TypeComputerUtilities.setRequiredAndInputTypes(funcExpr, requiredListType, inputListType);
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();

        IAType itemType = requiredListType.getItemType();
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
        return true;
    }

    /**
     * This method statically cast the type of records from their current type to the required type.
     * 
     * @param func
     *            The record constructor expression.
     * @param reqType
     *            The required type.
     * @param inputType
     *            The current type.
     * @param env
     *            The type environment.
     * @throws AlgebricksException
     */
    private static void staticRecordTypeCast(AbstractFunctionCallExpression func, ARecordType reqType,
            ARecordType inputType, IVariableTypeEnvironment env) throws AlgebricksException {
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
                    //type matched
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

                    // match the optional type input for a non-optional field
                    // delay that to runtime by calling the not-null function
                    if (fieldType.getTypeTag() == ATypeTag.UNION
                            && NonTaggedFormatUtil.isOptionalField((AUnionType) fieldType)) {
                        IAType itemType = ((AUnionType) fieldType).getUnionList().get(
                                NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                        if (reqFieldType.equals(itemType)) {
                            fieldPermutation[j] = i;
                            openFields[i] = false;
                            matched = true;

                            ScalarFunctionCallExpression notNullFunc = new ScalarFunctionCallExpression(
                                    new AsterixFunctionInfo(AsterixBuiltinFunctions.NOT_NULL));
                            notNullFunc.getArguments().add(new MutableObject<ILogicalExpression>(arg));
                            //wrap the not null function to the original function
                            func.getArguments().get(2 * i + 1).setValue(notNullFunc);
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
