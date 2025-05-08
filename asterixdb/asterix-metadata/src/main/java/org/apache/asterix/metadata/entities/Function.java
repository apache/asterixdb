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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.utils.Creator;
import org.apache.asterix.om.types.TypeSignature;

public class Function implements IMetadataEntity<Function> {
    private static final long serialVersionUID = 5L;

    private final FunctionSignature signature;
    private final List<String> paramNames;
    private final List<TypeSignature> paramTypes;
    private final TypeSignature returnType;
    private final String body;
    private final String language;
    private final String kind;
    private final String libraryDatabaseName;
    private final DataverseName libraryDataverseName;
    private final String libraryName;
    private final List<String> externalIdentifier;
    private final Boolean deterministic; // null for SQL++ and AQL functions
    private final Boolean nullCall; // null for SQL++ and AQL functions
    private final Map<String, String> resources;
    private final List<List<DependencyFullyQualifiedName>> dependencies;
    private final Creator creator;
    private final boolean transform;

    public Function(FunctionSignature signature, List<String> paramNames, List<TypeSignature> paramTypes,
            TypeSignature returnType, String functionBody, String functionKind, String language,
            String libraryDatabaseName, DataverseName libraryDataverseName, String libraryName,
            List<String> externalIdentifier, Boolean nullCall, Boolean deterministic, Map<String, String> resources,
            List<List<DependencyFullyQualifiedName>> dependencies, Creator creator, boolean transform) {
        this.signature = signature;
        this.paramNames = paramNames;
        this.paramTypes = paramTypes;
        this.body = functionBody;
        this.returnType = returnType;
        this.language = language;
        this.kind = functionKind;
        this.libraryDatabaseName = libraryDatabaseName;
        this.libraryDataverseName = libraryDataverseName;
        this.libraryName = libraryName;
        this.externalIdentifier = externalIdentifier;
        this.nullCall = nullCall;
        this.deterministic = deterministic;
        this.resources = resources == null ? Collections.emptyMap() : resources;
        this.dependencies = dependencies == null
                ? Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList())
                : dependencies;
        this.creator = creator;
        this.transform = transform;
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    public String getDatabaseName() {
        return signature.getDatabaseName();
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

    public List<String> getParameterNames() {
        return paramNames;
    }

    /**
     * @return {@code null} for non-external functions;
     *  for external function the list may contain {@code null} which means 'any' type
     */
    public List<TypeSignature> getParameterTypes() {
        return paramTypes;
    }

    /**
     * @return {@code null} for non-external functions
     */
    public TypeSignature getReturnType() {
        return returnType;
    }

    public String getFunctionBody() {
        return body;
    }

    public String getLanguage() {
        return language;
    }

    public String getKind() {
        return kind;
    }

    public boolean isExternal() {
        return externalIdentifier != null;
    }

    public String getLibraryDatabaseName() {
        return libraryDatabaseName;
    }

    public DataverseName getLibraryDataverseName() {
        return libraryDataverseName;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public List<String> getExternalIdentifier() {
        return externalIdentifier;
    }

    public Boolean getNullCall() {
        return nullCall;
    }

    public Boolean getDeterministic() {
        return deterministic;
    }

    public Map<String, String> getResources() {
        return resources;
    }

    public List<List<DependencyFullyQualifiedName>> getDependencies() {
        return dependencies;
    }

    public Creator getCreator() {
        return creator;
    }

    public boolean isTransform() {
        return transform;
    }

    @Override
    public Function addToCache(MetadataCache cache) {
        return cache.addFunctionIfNotExists(this);
    }

    @Override
    public Function dropFromCache(MetadataCache cache) {
        return cache.dropFunction(this);
    }

    public static List<DependencyKind> DEPENDENCIES_SCHEMA =
            Arrays.asList(DependencyKind.DATASET, DependencyKind.FUNCTION, DependencyKind.TYPE, DependencyKind.SYNONYM);

    public static List<List<DependencyFullyQualifiedName>> createDependencies(
            List<DependencyFullyQualifiedName> datasetDependencies,
            List<DependencyFullyQualifiedName> functionDependencies,
            List<DependencyFullyQualifiedName> typeDependencies,
            List<DependencyFullyQualifiedName> synonymDependencies) {
        List<List<DependencyFullyQualifiedName>> depList = new ArrayList<>(DEPENDENCIES_SCHEMA.size());
        depList.add(datasetDependencies);
        depList.add(functionDependencies);
        depList.add(typeDependencies);
        if (!synonymDependencies.isEmpty()) {
            depList.add(synonymDependencies);
        }
        return depList;
    }
}
