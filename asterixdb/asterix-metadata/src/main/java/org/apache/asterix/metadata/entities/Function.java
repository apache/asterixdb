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
import java.util.List;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

public class Function implements IMetadataEntity<Function> {
    private static final long serialVersionUID = 1L;
    public static final String LANGUAGE_AQL = "AQL";
    public static final String LANGUAGE_SQLPP = "SQLPP";
    public static final String LANGUAGE_JAVA = "JAVA";

    public static final String RETURNTYPE_VOID = "VOID";
    public static final String NOT_APPLICABLE = "N/A";

    private final FunctionSignature signature;
    private final List<List<List<String>>> dependencies;
    private final List<String> arguments;
    private final String body;
    private final String returnType;
    private final String language;
    private final String kind;

    public Function(FunctionSignature signature, List<String> arguments, String returnType, String functionBody,
            String language, String functionKind, List<List<List<String>>> dependencies) {
        this.signature = signature;
        this.arguments = arguments;
        this.body = functionBody;
        this.returnType = returnType == null ? RETURNTYPE_VOID : returnType;
        this.language = language;
        this.kind = functionKind;
        if (dependencies == null) {
            this.dependencies = new ArrayList<>();
            this.dependencies.add(new ArrayList<>());
            this.dependencies.add(new ArrayList<>());
        } else {
            this.dependencies = dependencies;
        }
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    public String getDataverseName() {
        return signature.getNamespace();
    }

    public String getName() {
        return signature.getName();
    }

    public int getArity() {
        return signature.getArity();
    }

    public List<String> getArguments() {
        return arguments;
    }

    public List<List<List<String>>> getDependencies() {
        return dependencies;
    }

    public String getFunctionBody() {
        return body;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getLanguage() {
        return language;
    }

    public String getKind() {
        return kind;
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
