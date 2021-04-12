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
package org.apache.asterix.om.functions;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class ExternalFunctionInfo extends FunctionInfo implements IExternalFunctionInfo {

    private static final long serialVersionUID = 5L;

    private final FunctionKind kind;
    private final List<IAType> parameterTypes;
    private final IAType returnType;
    private final ExternalFunctionLanguage language;
    private final DataverseName libraryDataverseName;
    private final String libraryName;
    private final List<String> externalIdentifier;
    private final Map<String, String> resources;
    private final boolean nullCall;

    public ExternalFunctionInfo(FunctionIdentifier fid, FunctionKind kind, List<IAType> parameterTypes,
            IAType returnType, IResultTypeComputer rtc, ExternalFunctionLanguage language,
            DataverseName libraryDataverseName, String libraryName, List<String> externalIdentifier,
            Map<String, String> resources, boolean deterministic, boolean nullCall) {
        super(fid, rtc, deterministic);
        this.kind = kind;
        this.parameterTypes = parameterTypes;
        this.returnType = returnType;
        this.language = language;
        this.libraryDataverseName = libraryDataverseName;
        this.libraryName = libraryName;
        this.externalIdentifier = externalIdentifier;
        this.resources = resources;
        this.nullCall = nullCall;
    }

    @Override
    public FunctionKind getKind() {
        return kind;
    }

    @Override
    public List<IAType> getParameterTypes() {
        return parameterTypes;
    }

    @Override
    public IAType getReturnType() {
        return returnType;
    }

    @Override
    public ExternalFunctionLanguage getLanguage() {
        return language;
    }

    public DataverseName getLibraryDataverseName() {
        return libraryDataverseName;
    }

    @Override
    public String getLibraryName() {
        return libraryName;
    }

    @Override
    public List<String> getExternalIdentifier() {
        return externalIdentifier;
    }

    @Override
    public Map<String, String> getResources() {
        return resources;
    }

    @Override
    public boolean getNullCall() {
        return nullCall;
    }
}
