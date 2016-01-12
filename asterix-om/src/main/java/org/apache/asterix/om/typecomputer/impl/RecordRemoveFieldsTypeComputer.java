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

import java.io.IOException;
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
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
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
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * Cases to support:
 * remove-fields($record, ["foo", ["bar", "access"]]),
 * where ["bar", "access"] is equivalent to the path bar->access
 */
public class RecordRemoveFieldsTypeComputer implements IResultTypeComputer {

    public static final RecordRemoveFieldsTypeComputer INSTANCE = new RecordRemoveFieldsTypeComputer();

    private RecordRemoveFieldsTypeComputer() {
    }

    private void getPathFromConstantExpression(ILogicalExpression expression, Set<String> fieldNameSet,
            List<List<String>> pathList) throws AlgebricksException {
        ConstantExpression ce = (ConstantExpression) expression;
        if (!(ce.getValue() instanceof AsterixConstantValue)) {
            throw new AlgebricksException("Expecting a list of strings and found " + ce.getValue() + " instead.");
        }
        IAObject item = ((AsterixConstantValue) ce.getValue()).getObject();
        ATypeTag type = item.getType().getTypeTag();

        switch (type) {
            case STRING:
                String fn = ((AString) item).getStringValue();
                fieldNameSet.add(fn);
                break;
            case ORDEREDLIST:
                AOrderedList pathOrdereList = (AOrderedList) item;
                String fieldName = ((AString) pathOrdereList.getItem(0)).getStringValue();
                fieldNameSet.add(fieldName);
                List<String> path = new ArrayList<>();
                for (int i = 0; i < pathOrdereList.size(); i++) {
                    path.add(((AString) pathOrdereList.getItem(i)).getStringValue());
                }
                pathList.add(path);
                break;
            default:
                throw new AlgebricksException("Unsupport type: " + type);
        }
    }

    private List<String> getListFromExpression(ILogicalExpression expression) throws AlgebricksException {
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> args = funcExp.getArguments();

        List<String> list = new ArrayList<>();
        for (Mutable<ILogicalExpression> arg : args) {
            // At this point all elements has to be a constant
            // Input list has only one level of nesting (list of list or list of strings)
            ConstantExpression ce = (ConstantExpression) arg.getValue();
            if (!(ce.getValue() instanceof AsterixConstantValue)) {
                throw new AlgebricksException("Expecting a list of strings and found " + ce.getValue() + " instead.");
            }
            IAObject item = ((AsterixConstantValue) ce.getValue()).getObject();
            ATypeTag type = item.getType().getTypeTag();
            if (type == ATypeTag.STRING) {
                list.add(((AString) item).getStringValue());
            } else {
                throw new AlgebricksException(type + " is currently not supported. Please check your function call.");
            }
        }

        return list;
    }

    private void getPathFromFunctionExpression(ILogicalExpression expression, Set<String> fieldNameSet,
            List<List<String>> pathList) throws AlgebricksException {

        List<String> path = getListFromExpression(expression);
        // Add the path head to remove set
        fieldNameSet.add(path.get(0));
        pathList.add(path);

    }

    private void computeTypeFromNonConstantExpression(ILogicalExpression expression, Set<String> fieldNameSet,
            List<List<String>> pathList) throws AlgebricksException {
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> args = funcExp.getArguments();

        for (Mutable<ILogicalExpression> arg : args) {
            ILogicalExpression le = arg.getValue();
            switch (le.getExpressionTag()) {
                case CONSTANT:
                    getPathFromConstantExpression(le, fieldNameSet, pathList);
                    break;
                case FUNCTION_CALL:
                    getPathFromFunctionExpression(le, fieldNameSet, pathList);
                    break;
                default:
                    throw new AlgebricksException("Unsupported expression: " + le);
            }
        }
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        IAType type0 = (IAType) env.getType(funcExpr.getArguments().get(0).getValue());

        List<List<String>> pathList = new ArrayList<>();
        Set<String> fieldNameSet = new HashSet<>();
        Deque<String> fieldPathStack = new ArrayDeque<>();

        ARecordType inputRecordType = NonTaggedFieldAccessByNameResultType.getRecordTypeFromType(type0, expression);
        if (inputRecordType == null) {
            return BuiltinType.ANY;
        }

        AbstractLogicalExpression arg1 = (AbstractLogicalExpression) funcExpr.getArguments().get(1).getValue();
        IAType inputListType = (IAType) env.getType(arg1);
        AOrderedListType inputOrderedListType = TypeComputerUtils.extractOrderedListType(inputListType);
        if (inputOrderedListType == null) {
            throw new AlgebricksException(
                    "The function 'remove-fields' expects an ordered list as the second argument, but got "
                            + inputListType);
        }

        ATypeTag tt = inputOrderedListType.getItemType().getTypeTag();
        if (tt == ATypeTag.STRING) { // If top-fieldlist
            if (setFieldNameSet(arg1, fieldNameSet)) {
                return buildOutputType(fieldPathStack, inputRecordType, fieldNameSet, pathList);
            } else {
                return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
        } else { // tt == ATypeTag.ANY, meaning the list is nested
            computeTypeFromNonConstantExpression(arg1, fieldNameSet, pathList);
            IAType resultType = buildOutputType(fieldPathStack, inputRecordType, fieldNameSet, pathList);
            return resultType;
        }
    }

    private boolean setFieldNameSet(ILogicalExpression expr, Set<String> fieldNameSet) {
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            AOrderedList orderedList = (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) expr).getValue())
                    .getObject());
            for (int i = 0; i < orderedList.size(); i++) {
                AString as = (AString) orderedList.getItem(i);
                fieldNameSet.add(as.getStringValue());
            }
            return true; // Success
        }
        return false;
    }

    private void addField(ARecordType inputRecordType,  String fieldName, List<String> resultFieldNames, List<IAType>
            resultFieldTypes)
            throws AlgebricksException {
        try {
            resultFieldNames.add(fieldName);
            if (inputRecordType.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                ARecordType nestedType = (ARecordType) inputRecordType.getFieldType(fieldName);
                //Deep Copy prevents altering of input types
                resultFieldTypes.add(nestedType.deepCopy(nestedType));
            } else {
                resultFieldTypes.add(inputRecordType.getFieldType(fieldName));
            }

        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    private IAType buildOutputType(Deque<String> fieldPathStack, ARecordType inputRecordType, Set<String> fieldNameSet,
            List<List<String>> pathList) throws AlgebricksException {
        IAType resultType;
        List<String> resultFieldNames = new ArrayList<>();
        List<IAType> resultFieldTypes = new ArrayList<>();

        String[] fieldNames = inputRecordType.getFieldNames();
        IAType[] fieldTypes = inputRecordType.getFieldTypes();

        for (int i = 0; i < fieldNames.length; i++) {
            if (!fieldNameSet.contains(fieldNames[i])) { // The main field is to be kept
                addField(inputRecordType, fieldNames[i], resultFieldNames, resultFieldTypes);
            } else if (!pathList.isEmpty()) { // Further check needed for nested fields
                if (fieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
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
    private <E> boolean isEqualPaths(List<E> l1, Deque<E> l2) {
        if ((l1 == null) || (l2 == null))
            return false;

        if (l1.size() != l2.size())
            return false;

        Iterator<E> it2 = l2.iterator();

        int len = l1.size();
        for (int i = len - 1; i >= 0; i--) {
            E o1 = l1.get(i);
            E o2 = it2.next();
            if (!o1.equals(o2))
                return false;
        }
        return true;
    }

    private boolean isRemovePath(Deque<String> fieldPath, List<List<String>> pathList) {
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
    private ARecordType deepCheckAndCopy(Deque<String> fieldPath, ARecordType srcRecType, List<List<String>>
            pathList, boolean isOpen)
            throws AlgebricksException {
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
                if (srcFieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
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

}