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

package org.apache.asterix.om.typecomputer.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.exceptions.InvalidExpressionException;
import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.exceptions.UnsupportedTypeException;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Cases to support:
 * remove-fields($record, ["foo", ["bar", "access"]]),
 * where ["bar", "access"] is equivalent to the path bar->access
 */
public class RecordRemoveFieldsTypeComputer implements IResultTypeComputer {

    public static final RecordRemoveFieldsTypeComputer INSTANCE = new RecordRemoveFieldsTypeComputer();

    private RecordRemoveFieldsTypeComputer() {
    }

    private static void getPathFromConstantExpression(FunctionIdentifier funcId, ILogicalExpression fieldNameExpression,
            Set<String> fieldNameSet, List<List<String>> pathList, SourceLocation sourceLoc)
            throws AlgebricksException {
        ConstantExpression ce = (ConstantExpression) fieldNameExpression;
        if (!(ce.getValue() instanceof AsterixConstantValue)) {
            throw new InvalidExpressionException(sourceLoc, funcId, 1, ce, LogicalExpressionTag.CONSTANT);
        }
        IAObject fieldName = ((AsterixConstantValue) ce.getValue()).getObject();
        getPathOfFieldName(funcId, fieldName, fieldNameSet, pathList, sourceLoc);
    }

    private static void getPathOfFieldName(FunctionIdentifier funcId, IAObject fieldName, Set<String> fieldNameSet,
            List<List<String>> pathList, SourceLocation sourceLoc) throws UnsupportedTypeException {
        ATypeTag type = fieldName.getType().getTypeTag();
        switch (type) {
            case STRING:
                String fn = ((AString) fieldName).getStringValue();
                fieldNameSet.add(fn);
                break;
            case ARRAY:
                AOrderedList pathOrdereList = (AOrderedList) fieldName;
                fieldNameSet.add(((AString) pathOrdereList.getItem(0)).getStringValue());
                List<String> path = new ArrayList<>();
                for (int i = 0; i < pathOrdereList.size(); i++) {
                    path.add(((AString) pathOrdereList.getItem(i)).getStringValue());
                }
                pathList.add(path);
                break;
            default:
                throw new UnsupportedTypeException(sourceLoc, funcId, type);
        }
    }

    private static List<String> getListFromExpression(FunctionIdentifier funcId, ILogicalExpression expression,
            SourceLocation sourceLoc) throws AlgebricksException {
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> args = funcExp.getArguments();

        List<String> list = new ArrayList<>();
        for (Mutable<ILogicalExpression> arg : args) {
            // At this point all elements has to be a constant
            // Input list has only one level of nesting (list of list or list of strings)
            ConstantExpression ce = (ConstantExpression) arg.getValue();
            if (!(ce.getValue() instanceof AsterixConstantValue)) {
                throw new InvalidExpressionException(sourceLoc, funcId, 1, ce, LogicalExpressionTag.CONSTANT);
            }
            IAObject item = ((AsterixConstantValue) ce.getValue()).getObject();
            ATypeTag type = item.getType().getTypeTag();
            if (type == ATypeTag.STRING) {
                list.add(((AString) item).getStringValue());
            } else {
                throw new UnsupportedTypeException(sourceLoc, funcId, type);
            }
        }

        return list;
    }

    private static void getPathFromFunctionExpression(FunctionIdentifier funcId, ILogicalExpression expression,
            Set<String> fieldNameSet, List<List<String>> pathList, SourceLocation sourceLoc)
            throws AlgebricksException {
        List<String> path = getListFromExpression(funcId, expression, sourceLoc);
        // Add the path head to remove set
        fieldNameSet.add(path.get(0));
        pathList.add(path);

    }

    private static void computeTypeFromExpression(FunctionIdentifier funcId, ILogicalExpression e,
            Set<String> fieldNameSet, List<List<String>> pathList) throws AlgebricksException {
        // e is the field to remove, ["foo", "bar"] = foo.bar, either an ordered list function call or a constant array
        SourceLocation sourceLocation = e.getSourceLocation();
        if (e.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            AOrderedList list = (AOrderedList) ((AsterixConstantValue) ((ConstantExpression) e).getValue()).getObject();
            for (int i = 0, size = list.size(); i < size; i++) {
                getPathOfFieldName(funcId, list.getItem(i), fieldNameSet, pathList, sourceLocation);
            }
        } else {
            AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) e;
            List<Mutable<ILogicalExpression>> args = funcExp.getArguments();

            for (Mutable<ILogicalExpression> arg : args) {
                ILogicalExpression le = arg.getValue();
                switch (le.getExpressionTag()) {
                    case CONSTANT:
                        getPathFromConstantExpression(funcId, le, fieldNameSet, pathList, sourceLocation);
                        break;
                    case FUNCTION_CALL:
                        getPathFromFunctionExpression(funcId, le, fieldNameSet, pathList, sourceLocation);
                        break;
                    default:
                        throw new InvalidExpressionException(sourceLocation, funcId, 1, le,
                                LogicalExpressionTag.CONSTANT, LogicalExpressionTag.FUNCTION_CALL);
                }
            }
        }
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        FunctionIdentifier funcId = funcExpr.getFunctionIdentifier();

        IAType type0 = (IAType) env.getType(funcExpr.getArguments().get(0).getValue());
        List<List<String>> pathList = new ArrayList<>();
        Set<String> fieldNameSet = new HashSet<>();
        Deque<String> fieldPathStack = new ArrayDeque<>();

        ARecordType inputRecordType = getRecordTypeFromType(funcId, type0, funcExpr.getSourceLocation());
        if (inputRecordType == null) {
            return BuiltinType.ANY;
        }

        AbstractLogicalExpression arg1 = (AbstractLogicalExpression) funcExpr.getArguments().get(1).getValue();
        IAType inputListType = (IAType) env.getType(arg1);
        AOrderedListType inputOrderedListType = TypeComputeUtils.extractOrderedListType(inputListType);
        if (inputOrderedListType == null) {
            throw new TypeMismatchException(funcExpr.getSourceLocation(), funcId, 1, inputListType.getTypeTag(),
                    ATypeTag.ARRAY);
        }

        ATypeTag tt = inputOrderedListType.getItemType().getTypeTag();
        if (tt == ATypeTag.STRING) {
            // all fields to be removed are top-level fields, e.g. remove_fields(obj, ["f1", "f2", "f3"])
            if (setFieldNameSet(arg1, fieldNameSet)) {
                return buildOutputType(fieldPathStack, inputRecordType, fieldNameSet, pathList);
            } else {
                return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
        } else {
            // some fields to be removed are nested, e.g. remove_fields(obj, ["f1", ["f2", "nested_field"], "f3"])
            computeTypeFromExpression(funcId, arg1, fieldNameSet, pathList);
            return buildOutputType(fieldPathStack, inputRecordType, fieldNameSet, pathList);
        }
    }

    private static boolean setFieldNameSet(ILogicalExpression expr, Set<String> fieldNameSet) {
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            AOrderedList orderedList =
                    (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) expr).getValue()).getObject());
            for (int i = 0; i < orderedList.size(); i++) {
                AString as = (AString) orderedList.getItem(i);
                fieldNameSet.add(as.getStringValue());
            }
            return true; // Success
        }
        return false;
    }

    private static void addField(ARecordType inputRecordType, String fieldName, List<String> resultFieldNames,
            List<IAType> resultFieldTypes) {
        resultFieldNames.add(fieldName);
        if (inputRecordType.getFieldType(fieldName).getTypeTag() == ATypeTag.OBJECT) {
            ARecordType nestedType = (ARecordType) inputRecordType.getFieldType(fieldName);
            //Deep Copy prevents altering of input types
            resultFieldTypes.add(nestedType.deepCopy(nestedType));
        } else {
            resultFieldTypes.add(inputRecordType.getFieldType(fieldName));
        }
    }

    private static IAType buildOutputType(Deque<String> fieldPathStack, ARecordType inputRecordType,
            Set<String> fieldNameSet, List<List<String>> pathList) {
        List<String> resultFieldNames = new ArrayList<>();
        List<IAType> resultFieldTypes = new ArrayList<>();

        String[] fieldNames = inputRecordType.getFieldNames();
        IAType[] fieldTypes = inputRecordType.getFieldTypes();

        for (int i = 0; i < fieldNames.length; i++) {
            if (!fieldNameSet.contains(fieldNames[i])) { // The main field is to be kept
                addField(inputRecordType, fieldNames[i], resultFieldNames, resultFieldTypes);
            } else if (!pathList.isEmpty() && fieldTypes[i].getTypeTag() == ATypeTag.OBJECT) {
                ARecordType subRecord = (ARecordType) fieldTypes[i];
                fieldPathStack.push(fieldNames[i]);
                subRecord = deepCheckAndCopy(fieldPathStack, subRecord, pathList, inputRecordType.isOpen());
                fieldPathStack.pop();
                if (subRecord != null) {
                    resultFieldNames.add(fieldNames[i]);
                    resultFieldTypes.add(subRecord);
                }
            }
        }

        int n = resultFieldNames.size();
        String resultTypeName = "result-record(" + inputRecordType.getTypeName() + ")";

        return new ARecordType(resultTypeName, resultFieldNames.toArray(new String[n]),
                resultFieldTypes.toArray(new IAType[n]), true); // Make the output type open always

    }

    /**
     * Comparison elements of two paths
     * Note: l2 uses a LIFO insert and removal.
     */
    private static <E> boolean isEqualPaths(List<E> l1, Deque<E> l2) {
        if ((l1 == null) || (l2 == null)) {
            return false;
        }

        if (l1.size() != l2.size()) {
            return false;
        }

        Iterator<E> it2 = l2.iterator();

        int len = l1.size();
        for (int i = len - 1; i >= 0; i--) {
            E o1 = l1.get(i);
            E o2 = it2.next();
            if (!o1.equals(o2)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isRemovePath(Deque<String> fieldPath, List<List<String>> pathList) {
        for (List<String> removePath : pathList) {
            if (isEqualPaths(removePath, fieldPath)) {
                return true;
            }
        }
        return false;
    }

    /*
        A method to deep copy a record the path validation
             i.e., keep only fields that are valid
     */
    private static ARecordType deepCheckAndCopy(Deque<String> fieldPath, ARecordType srcRecType,
            List<List<String>> pathList, boolean isOpen) {
        // Make sure the current path is valid before going further
        if (isRemovePath(fieldPath, pathList)) {
            return null;
        }

        String srcFieldNames[] = srcRecType.getFieldNames();
        IAType srcFieldTypes[] = srcRecType.getFieldTypes();

        List<IAType> destFieldTypes = new ArrayList<>();
        List<String> destFieldNames = new ArrayList<>();

        for (int i = 0; i < srcFieldNames.length; i++) {
            fieldPath.push(srcFieldNames[i]);
            if (!isRemovePath(fieldPath, pathList)) {
                if (srcFieldTypes[i].getTypeTag() == ATypeTag.OBJECT) {
                    ARecordType subRecord = (ARecordType) srcFieldTypes[i];
                    subRecord = deepCheckAndCopy(fieldPath, subRecord, pathList, isOpen);
                    if (subRecord != null) {
                        destFieldNames.add(srcFieldNames[i]);
                        destFieldTypes.add(subRecord);
                    }
                } else {
                    destFieldNames.add(srcFieldNames[i]);
                    destFieldTypes.add(srcFieldTypes[i]);
                }
            }
            fieldPath.pop();
        }

        int n = destFieldNames.size();
        if (n == 0) {
            return null;
        }
        return new ARecordType(srcRecType.getTypeName(), destFieldNames.toArray(new String[n]),
                destFieldTypes.toArray(new IAType[n]), isOpen);
    }

    private static ARecordType getRecordTypeFromType(FunctionIdentifier funcId, IAType type0, SourceLocation sourceLoc)
            throws AlgebricksException {
        switch (type0.getTypeTag()) {
            case OBJECT:
                return (ARecordType) type0;
            case ANY:
                return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            case UNION:
                IAType t1 = ((AUnionType) type0).getActualType();
                if (t1.getTypeTag() == ATypeTag.OBJECT) {
                    return (ARecordType) t1;
                } else if (t1.getTypeTag() == ATypeTag.ANY) {
                    return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                }
                // Falls through for other cases.
            default:
                throw new TypeMismatchException(sourceLoc, funcId, 0, type0.getTypeTag(), ATypeTag.OBJECT);
        }
    }
}
