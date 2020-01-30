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
package org.apache.asterix.metadata.entities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;

public class Function implements IMetadataEntity<Function> {
    private static final long serialVersionUID = 2L;
    public static final String LANGUAGE_AQL = "AQL";
    public static final String LANGUAGE_SQLPP = "SQLPP";
    public static final String LANGUAGE_JAVA = "JAVA";
    public static final String LANGUAGE_PYTHON = "PYTHON";

    private final FunctionSignature signature;
    private final List<List<Triple<DataverseName, String, String>>> dependencies;
    private final List<Pair<DataverseName, IAType>> arguments;
    private final String body;
    private final Pair<DataverseName, IAType> returnType;
    private final List<String> argNames;
    private final String language;
    private final String kind;
    private final String library;
    private final boolean nullable;
    private final Map<String, String> params;
    private final boolean deterministic;
    private final boolean nullCall;

    public Function(FunctionSignature signature, List<Pair<DataverseName, IAType>> arguments, List<String> argNames,
            Pair<DataverseName, IAType> returnType, String functionBody, String language, boolean unknownable,
            boolean nullCall, boolean deterministic, String library, String functionKind,
            List<List<Triple<DataverseName, String, String>>> dependencies, Map<String, String> params) {
        this.signature = signature;
        this.arguments = arguments;
        this.argNames = argNames;
        this.body = functionBody;
        this.returnType = returnType;
        this.language = language;
        this.nullable = unknownable;
        this.kind = functionKind;
        this.library = library;
        if (dependencies == null) {
            this.dependencies = new ArrayList<>(2);
            this.dependencies.add(Collections.emptyList());
            this.dependencies.add(Collections.emptyList());
        } else {
            this.dependencies = dependencies;
        }
        if (params == null) {
            this.params = new HashMap<>();
        } else {
            this.params = params;
        }
        this.nullCall = nullCall;
        this.deterministic = deterministic;
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    public DataverseName getDataverseName() {
        return signature.getDataverseName();
    }

    public String getName() {
        return signature.getName();
    }

    public int getArity() {
        return signature.getArity();
    }

    public List<Pair<DataverseName, IAType>> getArguments() {
        return arguments;
    }

    public List<String> getArgNames() {
        return argNames;
    }

    public List<List<Triple<DataverseName, String, String>>> getDependencies() {
        return dependencies;
    }

    public String getFunctionBody() {
        return body;
    }

    public Pair<DataverseName, IAType> getReturnType() {
        return returnType;
    }

    public String getLanguage() {
        return language;
    }

    public boolean isUnknownable() {
        return nullable;
    }

    public boolean isNullCall() {
        return nullCall;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public String getKind() {
        return kind;
    }

    public String getLibrary() {
        return library;
    }

    public Map<String, String> getParams() {
        return params;
    }

    @Override
    public Function addToCache(MetadataCache cache) {
        return cache.addFunctionIfNotExists(this);
    }

    @Override
    public Function dropFromCache(MetadataCache cache) {
        return cache.dropFunction(this);
    }

}
