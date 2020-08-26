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
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
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
import org.apache.hyracks.api.exceptions.SourceLocation;

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

        List<IAType> paramTypes = getParameterTypes(function, metadataProvider);

        IAType returnType = getType(function.getReturnType(), metadataProvider);

        IResultTypeComputer typeComputer = new ExternalTypeComputer(returnType, paramTypes);

        ExternalFunctionLanguage lang = getExternalFunctionLanguage(function.getLanguage());

        Boolean deterministic = function.getDeterministic();
        if (deterministic == null) {
            // all external functions should store 'deterministic' property
            throw new AsterixException(ErrorCode.METADATA_ERROR, function.getSignature().toString());
        }

        return new ExternalScalarFunctionInfo(function.getSignature().createFunctionIdentifier(), paramTypes,
                returnType, typeComputer, lang, function.getLibraryDataverseName(), function.getLibraryName(),
                function.getExternalIdentifier(), function.getResources(), deterministic);
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

    private static List<IAType> getParameterTypes(Function function, MetadataProvider metadataProvider)
            throws AlgebricksException {
        int arity = function.getArity();
        if (arity == 0) {
            return Collections.emptyList();
        } else if (arity >= 0) {
            List<IAType> types = new ArrayList<>(arity);
            List<TypeSignature> typeSignatures = function.getParameterTypes();
            if (typeSignatures != null) {
                if (typeSignatures.size() != arity) {
                    throw new AsterixException(ErrorCode.METADATA_ERROR, function.getSignature().toString());
                }
                for (TypeSignature ts : typeSignatures) {
                    IAType paramType = getType(ts, metadataProvider);
                    types.add(paramType);
                }
            } else {
                for (int i = 0; i < arity; i++) {
                    types.add(BuiltinType.ANY);
                }
            }
            return types;
        } else {
            // we don't yet support variadic external functions
            throw new AsterixException(ErrorCode.METADATA_ERROR, function.getSignature().toString());
        }
    }

    private static IAType getType(TypeSignature typeSignature, MetadataProvider metadataProvider)
            throws AlgebricksException {
        if (typeSignature == null) {
            return BuiltinType.ANY;
        }
        String typeName = typeSignature.getName();
        // back-compat: handle "any"
        if (BuiltinType.ANY.getTypeName().equals(typeName)) {
            return BuiltinType.ANY;
        }
        IAType type = BuiltinTypeMap.getBuiltinType(typeName);
        if (type == null) {
            type = metadataProvider.findType(typeSignature.getDataverseName(), typeName);
        }
        return type;
    }

    public static ExternalFunctionLanguage getExternalFunctionLanguage(String language) throws AsterixException {
        try {
            return ExternalFunctionLanguage.valueOf(language);
        } catch (IllegalArgumentException e) {
            throw new AsterixException(ErrorCode.METADATA_ERROR, language);
        }
    }

    public static void validateExternalIdentifier(List<String> externalIdentifier, ExternalFunctionLanguage language,
            SourceLocation sourceLoc) throws CompilationException {
        int expectedSize;
        switch (language) {
            case JAVA:
                expectedSize = 1;
                break;
            case PYTHON:
                expectedSize = 2;
                break;
            default:
                throw new CompilationException(ErrorCode.METADATA_ERROR, language.name());
        }
        int actualSize = externalIdentifier.size();
        if (actualSize != expectedSize) {
            throw new CompilationException(ErrorCode.INVALID_EXTERNAL_IDENTIFIER_SIZE, sourceLoc,
                    String.valueOf(actualSize), language.name());
        }
    }
}
