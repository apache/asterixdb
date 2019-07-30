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

package org.apache.asterix.optimizer.rules.typecast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.common.TypeResolverUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

/**
 * This class is utility to do type cast.
 * It offers two public methods:
 * 1. public static boolean rewriteListExpr(AbstractFunctionCallExpression funcExpr, IAType reqType, IAType inputType,
 * IVariableTypeEnvironment env) throws AlgebricksException, which only enforces the list type recursively.
 * 2. public static boolean rewriteFuncExpr(AbstractFunctionCallExpression funcExpr, IAType reqType, IAType inputType,
 * IVariableTypeEnvironment env) throws AlgebricksException, which enforces the list type and the record type
 * recursively.
 *
 * @author yingyib
 */
public class StaticTypeCastUtil {

    private StaticTypeCastUtil() {
    }

    /**
     * This method is only called when funcExpr contains list constructor function calls.
     * The List constructor is very special because a nested list is of type List<ANY>.
     * However, the bottom-up type inference (InferTypeRule in algebricks) did not infer that so we need this method to
     * enforce the type. We do not want to break the generality of algebricks so this method is called in an ASTERIX
     * rule: @ IntroduceEnforcedListTypeRule.
     *
     * @param funcExpr
     *            record constructor function expression
     * @param reqType
     *            required record type
     * @param inputType
     * @param env
     *            type environment
     * @throws AlgebricksException
     */
    public static boolean rewriteListExpr(AbstractFunctionCallExpression funcExpr, IAType reqType, IAType inputType,
            IVariableTypeEnvironment env) throws AlgebricksException {
        if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR) {
            if (reqType.equals(BuiltinType.ANY)) {
                reqType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
            }
            return rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType,
                    env);
        } else if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.ORDERED_LIST_CONSTRUCTOR) {
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
                    changed = rewriteListExpr(argFuncExpr, exprType, exprType, env) || changed;
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
     * However, the bottom-up type inference (InferTypeRule in algebricks) did not infer that so we need this method
     * to enforce the type. We do not want to break the generality of algebricks so this method is called in an
     * ASTERIX rule: @ IntroduceStaticTypeCastRule
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
        /**
         * sanity check: if there are list(ordered or unordered)/record variable expressions in the funcExpr, we will
         * not do STATIC type casting because they are not "statically cast-able".
         * instead, the record will be dynamically casted at the runtime
         */
        if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR) {
            if (reqType.equals(BuiltinType.ANY)) {
                reqType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
            }
            return rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType,
                    env);
        } else if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.ORDERED_LIST_CONSTRUCTOR) {
            if (reqType.equals(BuiltinType.ANY)) {
                reqType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            return rewriteListFuncExpr(funcExpr, (AbstractCollectionType) reqType, (AbstractCollectionType) inputType,
                    env);
        } else if (inputType.getTypeTag().equals(ATypeTag.OBJECT)) {
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
                    changed = rewriteFuncExpr(argFuncExpr, exprType, exprType, env) || changed;
                }
            }
            if (!compatible(reqType, inputType)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, funcExpr.getSourceLocation(),
                        "type mismatch, required: " + reqType.toString() + " actual: " + inputType.toString());
            }
            return changed;
        }
    }

    /**
     * only called when funcExpr is record constructor
     *
     * @param funcExpr
     *            record constructor function expression
     * @param requiredRecordType
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
        if (TypeCastUtils.getRequiredType(funcExpr) != null) {
            return false;
        }
        boolean casted = staticRecordTypeCast(funcExpr, requiredRecordType, inputRecordType, env);
        if (casted) {
            //enforce the required type if it is statically casted
            TypeCastUtils.setRequiredAndInputTypes(funcExpr, requiredRecordType, inputRecordType);
        }
        return casted;
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
        if (TypeCastUtils.getRequiredType(funcExpr) != null) {
            return false;
        }

        TypeCastUtils.setRequiredAndInputTypes(funcExpr, requiredListType, inputListType);
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        // TODO: if required type = [bigint], input type = [small_int], how is this method enforcing that?
        // TODO: it seems it's only concerned with dealing with complex types items (records,lists)
        IAType requiredItemType = requiredListType.getItemType();
        IAType inputItemType = inputListType.getItemType();
        boolean changed = false;
        for (int j = 0; j < args.size(); j++) {
            Mutable<ILogicalExpression> argRef = args.get(j);
            ILogicalExpression arg = argRef.getValue();
            IAType currentItemType = (inputItemType == null || inputItemType == BuiltinType.ANY)
                    ? (IAType) env.getType(arg) : inputItemType;
            switch (arg.getExpressionTag()) {
                case FUNCTION_CALL:
                    ScalarFunctionCallExpression argFunc = (ScalarFunctionCallExpression) arg;
                    changed |= rewriteFuncExpr(argFunc, requiredItemType, currentItemType, env);
                    changed |= castItem(argRef, argFunc, requiredItemType, env);
                    break;
                case VARIABLE:
                    // TODO(ali): why are we always casting to an open type without considering "requiredItemType"?
                    changed |= injectCastToRelaxType(argRef, currentItemType, env);
                    break;
            }
        }
        return changed;
    }

    private static boolean castItem(Mutable<ILogicalExpression> itemExprRef, ScalarFunctionCallExpression itemExpr,
            IAType requiredItemType, IVariableTypeEnvironment env) throws AlgebricksException {
        IAType itemType = (IAType) env.getType(itemExpr);
        if (TypeResolverUtil.needsCast(requiredItemType, itemType) && !satisfied(requiredItemType, itemType)) {
            injectCastFunction(FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE), requiredItemType, itemType,
                    itemExprRef, itemExpr);
            return true;
        }
        return false;
    }

    private static boolean satisfied(IAType required, IAType actual) {
        return required.getTypeTag() == ATypeTag.ANY && TypeHelper.isFullyOpen(actual);
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
    private static boolean staticRecordTypeCast(AbstractFunctionCallExpression func, ARecordType reqType,
            ARecordType inputType, IVariableTypeEnvironment env) throws AlgebricksException {
        if (!(func.getFunctionIdentifier() == BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR
                || func.getFunctionIdentifier() == BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
            return false;
        }

        List<Mutable<ILogicalExpression>> arguments = func.getArguments();
        int fieldArgumentCount = arguments.size() / 2;

        IAType[] reqFieldTypes = reqType.getFieldTypes();
        String[] reqFieldNames = reqType.getFieldNames();
        IAType[] inputFieldTypes = inputType.getFieldTypes();
        String[] inputFieldNames = inputType.getFieldNames();

        int[] fieldPermutation = new int[reqFieldTypes.length];
        BitSet nullFields = new BitSet(reqFieldTypes.length);
        BitSet openFields = new BitSet(fieldArgumentCount);

        Arrays.fill(fieldPermutation, -1);
        openFields.set(0, fieldArgumentCount);

        // forward match: match from actual to required
        for (int i = 0; i < inputFieldNames.length; i++) {
            String fieldName = inputFieldNames[i];
            IAType fieldType = inputFieldTypes[i];

            int fieldNameArgumentIdx = findFieldNameArgumentIdx(arguments, fieldName);
            if (fieldNameArgumentIdx < 0) {
                return false;
            }
            int fieldValueArgumentIdx = fieldNameArgumentIdx + 1;
            int fieldArgumentIdx = fieldNameArgumentIdx / 2;

            ILogicalExpression arg = arguments.get(fieldValueArgumentIdx).getValue();
            boolean matched = false;
            for (int j = 0; j < reqFieldNames.length; j++) {
                String reqFieldName = reqFieldNames[j];
                IAType reqFieldType = reqFieldTypes[j];
                if (fieldName.equals(reqFieldName)) {
                    //type matched
                    if (fieldType.equals(reqFieldType)) {
                        fieldPermutation[j] = fieldNameArgumentIdx;
                        openFields.clear(fieldArgumentIdx);
                        matched = true;

                        if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            ScalarFunctionCallExpression scalarFunc = (ScalarFunctionCallExpression) arg;
                            rewriteFuncExpr(scalarFunc, reqFieldType, fieldType, env);
                        }
                        break;
                    }

                    // match the optional field
                    if (NonTaggedFormatUtil.isOptional(reqFieldType)) {
                        IAType itemType = ((AUnionType) reqFieldType).getActualType();
                        reqFieldType = itemType;
                        if (fieldType.equals(BuiltinType.AMISSING) || fieldType.equals(itemType)) {
                            fieldPermutation[j] = fieldNameArgumentIdx;
                            openFields.clear(fieldArgumentIdx);
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
                    if (NonTaggedFormatUtil.isOptional(fieldType)) {
                        IAType itemType = ((AUnionType) fieldType).getActualType();
                        if (reqFieldType.equals(itemType)) {
                            fieldPermutation[j] = fieldNameArgumentIdx;
                            openFields.clear(fieldArgumentIdx);
                            matched = true;

                            ScalarFunctionCallExpression notNullFunc = new ScalarFunctionCallExpression(
                                    FunctionUtil.getFunctionInfo(BuiltinFunctions.CHECK_UNKNOWN));
                            notNullFunc.getArguments().add(new MutableObject<>(arg));
                            //wrap the not null function to the original function
                            arguments.get(fieldValueArgumentIdx).setValue(notNullFunc);
                            break;
                        }
                    }

                    // match the record field: need cast
                    if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        ScalarFunctionCallExpression scalarFunc = (ScalarFunctionCallExpression) arg;
                        rewriteFuncExpr(scalarFunc, reqFieldType, fieldType, env);
                        fieldPermutation[j] = fieldNameArgumentIdx;
                        openFields.clear(fieldArgumentIdx);
                        matched = true;
                        break;
                    }
                }
            }
            // the input has extra fields
            if (!matched && !reqType.isOpen()) {
                throw new AlgebricksException("static type mismatch: the input record includes an extra closed field "
                        + fieldName + ":" + fieldType + "! Please check the field name and type.");
            }
        }

        // backward match: match from required to actual
        for (int i = 0; i < reqFieldNames.length; i++) {
            String reqFieldName = reqFieldNames[i];
            IAType reqFieldType = reqFieldTypes[i];
            boolean matched = false;
            for (int j = 0; j < inputFieldNames.length; j++) {
                String fieldName = inputFieldNames[j];
                IAType fieldType = inputFieldTypes[j];
                if (!fieldName.equals(reqFieldName)) {
                    continue;
                }
                // should check open field here
                // because number of entries in fieldPermutations is the
                // number of required schema fields
                // here we want to check if an input field is matched
                // the entry index of fieldPermutations is req field index
                int fieldNameArgumentIdx = findFieldNameArgumentIdx(arguments, fieldName);
                if (fieldNameArgumentIdx < 0) {
                    return false;
                }
                int fieldArgumentIdx = fieldNameArgumentIdx / 2;
                if (!openFields.get(fieldArgumentIdx)) {
                    matched = true;
                    break;
                }

                // match the optional field
                if (!NonTaggedFormatUtil.isOptional(reqFieldType)) {
                    continue;
                }
                IAType itemType = ((AUnionType) reqFieldType).getActualType();
                if (fieldType.equals(BuiltinType.AMISSING) || fieldType.equals(itemType)) {
                    matched = true;
                    break;
                }
            }
            if (matched) {
                continue;
            }

            if (NonTaggedFormatUtil.isOptional(reqFieldType)) {
                // add a null field
                nullFields.set(i);
            } else {
                // no matched field in the input for a required closed field
                if (inputType.isOpen()) {
                    //if the input type is open, return false, give that to dynamic type cast to defer the error to the runtime
                    return false;
                } else {
                    throw new AlgebricksException(
                            "static type mismatch: the input record misses a required closed field " + reqFieldName
                                    + ":" + reqFieldType + "! Please check the field name and type.");
                }
            }
        }

        List<Mutable<ILogicalExpression>> newArguments = new ArrayList<>(arguments.size());

        // re-order the closed part and fill in null fields
        for (int i = 0; i < reqFieldTypes.length; i++) {
            int pos = fieldPermutation[i];
            if (pos >= 0) {
                newArguments.add(arguments.get(pos));
                newArguments.add(arguments.get(pos + 1));
            }
            if (nullFields.get(i)) {
                // add a null field
                newArguments.add(new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AString(reqFieldNames[i])))));
                newArguments.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ANull.NULL))));
            }
        }

        // add the open part
        for (int i = openFields.nextSetBit(0); i >= 0; i = openFields.nextSetBit(i + 1)) {
            newArguments.add(arguments.get(i * 2));
            Mutable<ILogicalExpression> expRef = arguments.get(i * 2 + 1);
            IAType expType = (IAType) env.getType(expRef.getValue());
            injectCastToRelaxType(expRef, expType, env);
            newArguments.add(expRef);
        }

        arguments.clear();
        arguments.addAll(newArguments);

        return true;
    }

    private static int findFieldNameArgumentIdx(List<Mutable<ILogicalExpression>> arguments, String fieldName) {
        for (int i = 0, ln = arguments.size(); i < ln; i += 2) {
            String n = ConstantExpressionUtil.getStringConstant(arguments.get(i).getValue());
            if (n == null) {
                break;
            } else if (fieldName.equals(n)) {
                return i;
            }
        }
        return -1;
    }

    // casts exprRef (which is either a function call or a variable) to fully open if it is not already fully open
    private static boolean injectCastToRelaxType(Mutable<ILogicalExpression> expRef, IAType expType,
            IVariableTypeEnvironment env) throws AlgebricksException {
        ILogicalExpression argExpr = expRef.getValue();
        List<LogicalVariable> parameterVars = new ArrayList<>();
        argExpr.getUsedVariables(parameterVars);
        boolean castInjected = false;
        if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                || argExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            IAType exprActualType = expType;
            if (expType.getTypeTag() == ATypeTag.UNION) {
                exprActualType = ((AUnionType) expType).getActualType();
            }
            IAType requiredType = exprActualType;
            // do not enforce nested type in the case of no-used variables
            switch (exprActualType.getTypeTag()) {
                case OBJECT:
                    requiredType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                    break;
                case ARRAY:
                    requiredType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                    break;
                case MULTISET:
                    requiredType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
                    break;
                default:
                    break;
            }
            // add cast(expr) if the expr is a variable or using a variable or non constructor function call expr.
            // skip if expr is a constructor with values where you can traverse and cast fields/items individually
            if (!exprActualType.equals(requiredType) && (!parameterVars.isEmpty() || !isComplexConstructor(argExpr))) {
                injectCastFunction(FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE), requiredType, expType,
                        expRef, argExpr);
                castInjected = true;
            }
            //recursively rewrite function arguments
            if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                    && TypeCastUtils.getRequiredType((AbstractFunctionCallExpression) argExpr) == null) {
                AbstractFunctionCallExpression argFunc = (AbstractFunctionCallExpression) argExpr;
                if (castInjected) {
                    //rewrite the arg expression inside the dynamic cast
                    rewriteFuncExpr(argFunc, exprActualType, exprActualType, env);
                } else {
                    //rewrite arg
                    rewriteFuncExpr(argFunc, requiredType, exprActualType, env);
                }
            }
        }
        return castInjected;
    }

    private static boolean isComplexConstructor(ILogicalExpression expression) {
        if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            FunctionIdentifier funIdentifier = ((AbstractFunctionCallExpression) expression).getFunctionIdentifier();
            return funIdentifier.equals(BuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR)
                    || funIdentifier.equals(BuiltinFunctions.ORDERED_LIST_CONSTRUCTOR)
                    || funIdentifier.equals(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)
                    || funIdentifier.equals(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR);
        }
        return false;
    }

    /**
     * Inject a dynamic cast function wrapping an existing expression
     *
     * @param funcInfo
     *            the cast function
     * @param reqType
     *            the required type
     * @param inputType
     *            the original type
     * @param exprRef
     *            the expression reference
     * @param argExpr
     *            the original expression
     * @throws AlgebricksException if types are incompatible (tag-wise)
     */
    private static void injectCastFunction(IFunctionInfo funcInfo, IAType reqType, IAType inputType,
            Mutable<ILogicalExpression> exprRef, ILogicalExpression argExpr) throws AlgebricksException {
        ScalarFunctionCallExpression cast = new ScalarFunctionCallExpression(funcInfo);
        cast.getArguments().add(new MutableObject<>(argExpr));
        cast.setSourceLocation(argExpr.getSourceLocation());
        exprRef.setValue(cast);
        TypeCastUtils.setRequiredAndInputTypes(cast, reqType, inputType);
    }

    /**
     * Determine if two types are compatible
     *
     * @param reqType
     *            the required type
     * @param inputType
     *            the input type
     * @return true if the two types are compatible; false otherwise
     */
    public static boolean compatible(IAType reqType, IAType inputType) {
        if (reqType.getTypeTag() == ATypeTag.ANY || inputType.getTypeTag() == ATypeTag.ANY) {
            return true;
        }
        if (reqType.getTypeTag() != ATypeTag.UNION && inputType.getTypeTag() != ATypeTag.UNION) {
            if (reqType.equals(inputType)) {
                return true;
            } else {
                return false;
            }
        }
        Set<IAType> reqTypePossible = new HashSet<>();
        Set<IAType> inputTypePossible = new HashSet<>();
        if (reqType.getTypeTag() == ATypeTag.UNION) {
            AUnionType unionType = (AUnionType) reqType;
            reqTypePossible.addAll(unionType.getUnionList());
        } else {
            reqTypePossible.add(reqType);
        }

        if (inputType.getTypeTag() == ATypeTag.UNION) {
            AUnionType unionType = (AUnionType) inputType;
            inputTypePossible.addAll(unionType.getUnionList());
        } else {
            inputTypePossible.add(inputType);
        }
        return reqTypePossible.equals(inputTypePossible);
    }
}
