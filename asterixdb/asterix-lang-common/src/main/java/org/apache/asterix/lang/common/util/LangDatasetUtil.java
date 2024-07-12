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

package org.apache.asterix.lang.common.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;

public class LangDatasetUtil {
    private LangDatasetUtil() {
    }

    public static List<List<DependencyFullyQualifiedName>> getDatasetDependencies(MetadataProvider metadataProvider,
            DatasetDecl datasetDecl, IQueryRewriter rewriter) throws CompilationException {
        Expression normBody = datasetDecl.getQuery().getBody();
        if (normBody == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, datasetDecl.getSourceLocation(),
                    datasetDecl.getName().toString());
        }

        List<DependencyFullyQualifiedName> datasetDependencies = new ArrayList<>();
        List<DependencyFullyQualifiedName> synonymDependencies = new ArrayList<>();
        List<DependencyFullyQualifiedName> functionDependencies = new ArrayList<>();
        ExpressionUtils.collectDependencies(metadataProvider, normBody, rewriter, datasetDependencies,
                synonymDependencies, functionDependencies);

        List<DependencyFullyQualifiedName> typeDependencies = Collections.emptyList();
        return Dataset.createDependencies(datasetDependencies, functionDependencies, typeDependencies,
                synonymDependencies);
    }
}
