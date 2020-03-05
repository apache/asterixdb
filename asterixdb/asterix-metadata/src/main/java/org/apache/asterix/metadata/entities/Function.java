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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Triple;

public class Function implements IMetadataEntity<Function> {
    private static final long serialVersionUID = 3L;

    private final FunctionSignature signature;
    private final List<String> argNames;
    private final List<IAType> argTypes;
    private final IAType returnType;
    private final String body;
    private final String language;
    private final String kind;
    private final String library;
    private final Boolean deterministic; // null for SQL++ and AQL functions
    private final Boolean nullCall; // null for SQL++ and AQL functions
    private final Map<String, String> params;
    private final List<List<Triple<DataverseName, String, String>>> dependencies;

    public Function(FunctionSignature signature, List<String> argNames, List<IAType> argTypes, IAType returnType,
            String functionBody, String functionKind, String language, String library, Boolean nullCall,
            Boolean deterministic, Map<String, String> params,
            List<List<Triple<DataverseName, String, String>>> dependencies) {
        this.signature = signature;
        this.argNames = argNames;
        this.argTypes = argTypes;
        this.body = functionBody;
        this.returnType = returnType;
        this.language = language;
        this.kind = functionKind;
        this.library = library;
        this.nullCall = nullCall;
        this.deterministic = deterministic;
        this.params = params == null ? new HashMap<>() : params;
        this.dependencies = dependencies == null
                ? Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList())
                : dependencies;
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

    public List<String> getArgNames() {
        return argNames;
    }

    public List<IAType> getArgTypes() {
        return argTypes;
    }

    public String getFunctionBody() {
        return body;
    }

    public IAType getReturnType() {
        return returnType;
    }

    public String getLanguage() {
        return language;
    }

    public String getKind() {
        return kind;
    }

    public boolean isExternal() {
        return library != null;
    }

    public String getLibrary() {
        return library;
    }

    public Boolean getNullCall() {
        return nullCall;
    }

    public Boolean getDeterministic() {
        return deterministic;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public List<List<Triple<DataverseName, String, String>>> getDependencies() {
        return dependencies;
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
