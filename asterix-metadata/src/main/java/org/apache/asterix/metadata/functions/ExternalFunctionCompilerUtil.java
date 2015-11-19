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
package org.apache.asterix.metadata.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.AsterixFunction;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ABinaryTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AFloatTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt32TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AStringTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class ExternalFunctionCompilerUtil {

    private static Pattern orderedListPattern = Pattern.compile("\\[*\\]");
    private static Pattern unorderedListPattern = Pattern.compile("[{{*}}]");

    public static IFunctionInfo getExternalFunctionInfo(MetadataTransactionContext txnCtx, Function function)
            throws MetadataException {

        String functionKind = function.getKind();
        IFunctionInfo finfo = null;
        if (FunctionKind.SCALAR.toString().equalsIgnoreCase(functionKind)) {
            finfo = getScalarFunctionInfo(txnCtx, function);
        } else if (FunctionKind.AGGREGATE.toString().equalsIgnoreCase(functionKind)) {
            finfo = getAggregateFunctionInfo(txnCtx, function);
        } else if (FunctionKind.STATEFUL.toString().equalsIgnoreCase(functionKind)) {
            finfo = getStatefulFunctionInfo(txnCtx, function);
        } else if (FunctionKind.UNNEST.toString().equalsIgnoreCase(functionKind)) {
            finfo = getUnnestFunctionInfo(txnCtx, function);
        }
        return finfo;
    }

    private static IFunctionInfo getScalarFunctionInfo(MetadataTransactionContext txnCtx, Function function)
            throws MetadataException {
        FunctionIdentifier fid = new FunctionIdentifier(function.getDataverseName(), function.getName(),
                function.getArity());
        IResultTypeComputer typeComputer = getResultTypeComputer(txnCtx, function);
        List<IAType> arguments = new ArrayList<IAType>();
        IAType returnType = null;
        List<String> paramTypes = function.getParams();
        for (String paramType : paramTypes) {
            arguments.add(getTypeInfo(paramType, txnCtx, function));
        }

        returnType = getTypeInfo(function.getReturnType(), txnCtx, function);

        AsterixExternalScalarFunctionInfo scalarFunctionInfo = new AsterixExternalScalarFunctionInfo(fid.getNamespace(),
                new AsterixFunction(fid.getName(), fid.getArity()), returnType, function.getFunctionBody(),
                function.getLanguage(), arguments, typeComputer);
        return scalarFunctionInfo;
    }

    private static IAType getTypeInfo(String paramType, MetadataTransactionContext txnCtx, Function function)
            throws MetadataException {
        if (paramType.equalsIgnoreCase(BuiltinType.AINT32.getDisplayName())) {
            return (BuiltinType.AINT32);
        } else if (paramType.equalsIgnoreCase(BuiltinType.AFLOAT.getDisplayName())) {
            return (BuiltinType.AFLOAT);
        } else if (paramType.equalsIgnoreCase(BuiltinType.ASTRING.getDisplayName())) {
            return (BuiltinType.ASTRING);
        } else if (paramType.equalsIgnoreCase(BuiltinType.ADOUBLE.getDisplayName())) {
            return (BuiltinType.ADOUBLE);
        } else {
            IAType collection = getCollectionType(paramType, txnCtx, function);
            if (collection != null) {
                return collection;
            } else {
                Datatype datatype;
                datatype = MetadataManager.INSTANCE.getDatatype(txnCtx, function.getDataverseName(), paramType);
                if (datatype == null) {
                    throw new MetadataException(" Type " + paramType + " not defined");
                }
                return (datatype.getDatatype());
            }
        }
    }

    private static IAType getCollectionType(String paramType, MetadataTransactionContext txnCtx, Function function)
            throws MetadataException {

        Matcher matcher = orderedListPattern.matcher(paramType);
        if (matcher.find()) {
            String subType = paramType.substring(paramType.indexOf('[') + 1, paramType.lastIndexOf(']'));
            return new AOrderedListType(getTypeInfo(subType, txnCtx, function), "AOrderedList");
        } else {
            matcher = unorderedListPattern.matcher(paramType);
            if (matcher.find()) {
                String subType = paramType.substring(paramType.indexOf("{{") + 2, paramType.lastIndexOf("}}"));
                return new AUnorderedListType(getTypeInfo(subType, txnCtx, function), "AUnorderedList");
            }
        }
        return null;
    }

    private static IResultTypeComputer getResultTypeComputer(final MetadataTransactionContext txnCtx,
            final Function function) throws MetadataException {

        final IAType type = getTypeInfo(function.getReturnType(), txnCtx, function);
        switch (type.getTypeTag()) {
            case INT32:
                return AInt32TypeComputer.INSTANCE;
            case FLOAT:
                return AFloatTypeComputer.INSTANCE;
            case DOUBLE:
                return ADoubleTypeComputer.INSTANCE;
            case BINARY:
                return ABinaryTypeComputer.INSTANCE;
            case STRING:
                return AStringTypeComputer.INSTANCE;
            case ORDEREDLIST:
                return new ExternalTypeComputer() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

                        return new AOrderedListType(((AOrderedListType) type).getItemType(),
                                ((AOrderedListType) type).getItemType().getTypeName());
                    }

                };
            case UNORDEREDLIST:
                return new ExternalTypeComputer() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

                        return new AUnorderedListType(type, type.getTypeName());
                    }

                };
            default:
                return new ExternalTypeComputer() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                            IMetadataProvider<?, ?> mp) throws AlgebricksException {
                        return type;
                    }
                };
        }

    }

    private static IFunctionInfo getUnnestFunctionInfo(MetadataTransactionContext txnCtx, Function function) {
        return null;
    }

    private static IFunctionInfo getStatefulFunctionInfo(MetadataTransactionContext txnCtx, Function function) {
        return null;
    }

    private static IFunctionInfo getAggregateFunctionInfo(MetadataTransactionContext txnCtx, Function function) {
        return null;
    }

}