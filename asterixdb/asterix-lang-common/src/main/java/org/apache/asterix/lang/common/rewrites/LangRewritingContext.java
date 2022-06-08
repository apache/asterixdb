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
package org.apache.asterix.lang.common.rewrites;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class LangRewritingContext {
    private final MetadataProvider metadataProvider;
    private final IWarningCollector warningCollector;
    private final Map<FunctionSignature, FunctionDecl> declaredFunctions;
    private final Map<DatasetFullyQualifiedName, ViewDecl> declaredViews;
    private final Counter varCounter;
    private int systemVarCounter = 1;
    private final Map<Integer, VarIdentifier> oldVarIdToNewVarId = new HashMap<>();

    public LangRewritingContext(MetadataProvider metadataProvider, List<FunctionDecl> declaredFunctions,
            List<ViewDecl> declaredViews, IWarningCollector warningCollector, int varCounter) {
        this.metadataProvider = metadataProvider;
        this.warningCollector = warningCollector;
        this.declaredFunctions = createMap(declaredFunctions, FunctionDecl::getSignature);
        this.declaredViews = createMap(declaredViews, ViewDecl::getViewName);
        this.varCounter = new Counter(varCounter);
    }

    public Counter getVarCounter() {
        return varCounter;
    }

    /**
     * Generate a new variable with the same identifier (varValue) but a different Id.
     *
     * @param oldId
     *            , the old variable id
     * @param varValue
     *            , the identifier
     * @return the new varible.
     */
    public VarIdentifier mapOldId(Integer oldId, String varValue) {
        int n = increaseAndGetCtr();
        VarIdentifier newVar = new VarIdentifier(varValue);
        newVar.setId(n);
        oldVarIdToNewVarId.put(oldId, newVar);
        return newVar;
    }

    public VarIdentifier getRewrittenVar(Integer oldId) {
        return oldVarIdToNewVarId.get(oldId);
    }

    public VarIdentifier newVariable() {
        int id = increaseAndGetCtr();
        // Prefixes system-generated variables with "#".
        return new VarIdentifier("#" + (systemVarCounter++), id);
    }

    private int increaseAndGetCtr() {
        varCounter.inc();
        return varCounter.get();
    }

    public IWarningCollector getWarningCollector() {
        return warningCollector;
    }

    public MetadataProvider getMetadataProvider() {
        return metadataProvider;
    }

    public Map<FunctionSignature, FunctionDecl> getDeclaredFunctions() {
        return declaredFunctions;
    }

    public Map<DatasetFullyQualifiedName, ViewDecl> getDeclaredViews() {
        return declaredViews;
    }

    private static <K, V> Map<K, V> createMap(List<V> values, java.util.function.Function<V, K> keyMapper) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<K, V> result = new HashMap<>();
        for (V v : values) {
            result.put(keyMapper.apply(v), v);
        }
        return Collections.unmodifiableMap(result);
    }
}
