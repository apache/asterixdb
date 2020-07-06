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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class ExternalFunctionCompilerUtil {

    private ExternalFunctionCompilerUtil() {
        // do nothing
    }

    public static IFunctionInfo getExternalFunctionInfo(MetadataProvider metadataProvider, Function function)
            throws AlgebricksException {

        String functionKind = function.getKind();
        IFunctionInfo finfo = null;
        if (FunctionKind.SCALAR.toString().equalsIgnoreCase(functionKind)) {
            finfo = getScalarFunctionInfo(metadataProvider, function);
        } else if (FunctionKind.AGGREGATE.toString().equalsIgnoreCase(functionKind)) {
            finfo = getAggregateFunctionInfo(metadataProvider, function);
        } else if (FunctionKind.STATEFUL.toString().equalsIgnoreCase(functionKind)) {
            finfo = getStatefulFunctionInfo(metadataProvider, function);
        } else if (FunctionKind.UNNEST.toString().equalsIgnoreCase(functionKind)) {
            finfo = getUnnestFunctionInfo(metadataProvider, function);
        }
        return finfo;
    }

    private static IFunctionInfo getScalarFunctionInfo(MetadataProvider metadataProvider, Function function)
            throws AlgebricksException {
        if (function.getDeterministic() == null) {
            throw new AsterixException(ErrorCode.METADATA_ERROR, "");
        }

        List<IAType> paramTypes = new ArrayList<>(function.getParameterTypes().size());
        for (TypeSignature ts : function.getParameterTypes()) {
            IAType paramType = resolveFunctionType(ts, metadataProvider);
            paramTypes.add(paramType);
        }

        IAType returnType = resolveFunctionType(function.getReturnType(), metadataProvider);

        IResultTypeComputer typeComputer = new ExternalTypeComputer(returnType, paramTypes);

        ExternalFunctionLanguage lang;
        try {
            lang = ExternalFunctionLanguage.valueOf(function.getLanguage());
        } catch (IllegalArgumentException e) {
            throw new AsterixException(ErrorCode.METADATA_ERROR, function.getLanguage());
        }
        List<String> externalIdentifier = decodeExternalIdentifier(lang, function.getFunctionBody());

        return new ExternalScalarFunctionInfo(function.getSignature().createFunctionIdentifier(), returnType,
                externalIdentifier, lang, function.getLibrary(), paramTypes, function.getResources(),
                function.getDeterministic(), typeComputer);
    }

    private static IFunctionInfo getUnnestFunctionInfo(MetadataProvider metadataProvider, Function function) {
        return null;
    }

    private static IFunctionInfo getStatefulFunctionInfo(MetadataProvider metadataProvider, Function function) {
        return null;
    }

    private static IFunctionInfo getAggregateFunctionInfo(MetadataProvider metadataProvider, Function function) {
        return null;
    }

    private static IAType resolveFunctionType(TypeSignature typeSignature, MetadataProvider metadataProvider)
            throws AlgebricksException {
        String typeName = typeSignature.getName();
        if (BuiltinType.ANY.getTypeName().equals(typeName)) {
            return BuiltinType.ANY;
        }
        IAType type = BuiltinTypeMap.getBuiltinType(typeName);
        if (type == null) {
            type = metadataProvider.findType(typeSignature.getDataverseName(), typeName);
        }
        return type;
    }

    public static String encodeExternalIdentifier(FunctionSignature functionSignature,
            ExternalFunctionLanguage language, List<String> identList) throws AlgebricksException {
        switch (language) {
            case JAVA:
                // input:
                // [0] = package.class
                //
                // output: package.class

                return identList.get(0);

            case PYTHON:
                // input: either a method or a top-level function
                // [0] = package.module(:class)?
                // [1] = (function_or_method)? - if missing then defaults to declared function name
                //
                // output:
                // case 1 (method): package.module:class.method
                // case 2 (function): package.module:function

                String ident0 = identList.get(0);
                String ident1 = identList.size() > 1 ? identList.get(1) : functionSignature.getName();
                boolean classExists = ident0.indexOf(':') > 0;
                return ident0 + (classExists ? '.' : ':') + ident1;

            default:
                throw new AsterixException(ErrorCode.COMPILATION_ERROR, language);
        }
    }

    public static List<String> decodeExternalIdentifier(ExternalFunctionLanguage language, String encodedValue)
            throws AlgebricksException {
        switch (language) {
            case JAVA:
                // input: class
                //
                // output:
                // [0] = class
                return Collections.singletonList(encodedValue);

            case PYTHON:
                // input:
                //  case 1 (method): package.module:class.method
                //  case 2 (function): package.module:function
                //
                // output:
                //  case 1:
                //    [0] = package.module
                //    [1] = class
                //    [2] = method
                //  case 2:
                //    [0] = package.module
                //    [1] = function

                int d1 = encodedValue.indexOf(':');
                if (d1 <= 0) {
                    throw new AsterixException(ErrorCode.COMPILATION_ERROR, encodedValue);
                }
                String moduleName = encodedValue.substring(0, d1);
                int d2 = encodedValue.lastIndexOf('.');
                if (d2 > d1) {
                    // class.method
                    String className = encodedValue.substring(d1 + 1, d2);
                    String methodName = encodedValue.substring(d2 + 1);
                    return Arrays.asList(moduleName, className, methodName);
                } else {
                    // function
                    String functionName = encodedValue.substring(d1 + 1);
                    return Arrays.asList(moduleName, functionName);
                }

            default:
                throw new AsterixException(ErrorCode.COMPILATION_ERROR, language);
        }
    }
}
