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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.metadata.entities.ViewDetails;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class ViewUtil {

    private ViewUtil() {
    }

    public static ViewDecl parseStoredView(DatasetFullyQualifiedName viewName, ViewDetails view,
            IParserFactory parserFactory, IWarningCollector warningCollector, SourceLocation sourceLoc)
            throws CompilationException {
        IParser parser = parserFactory.createParser(new StringReader(view.getViewBody()));
        try {
            ViewDecl viewDecl = parser.parseViewBody(viewName);
            viewDecl.setSourceLocation(sourceLoc);
            if (warningCollector != null) {
                parser.getWarnings(warningCollector);
            }
            return viewDecl;
        } catch (CompilationException e) {
            throw new CompilationException(ErrorCode.COMPILATION_BAD_VIEW_DEFINITION, e, sourceLoc, viewName,
                    e.getMessage());
        }
    }

    public static List<List<Triple<DataverseName, String, String>>> getViewDependencies(ViewDecl viewDecl,
            IQueryRewriter rewriter) throws CompilationException {
        Expression normBody = viewDecl.getNormalizedViewBody();
        if (normBody == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, viewDecl.getSourceLocation(),
                    viewDecl.getViewName().toString());
        }

        // Get the list of used functions and used datasets
        List<Triple<DataverseName, String, String>> datasetDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> synonymDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> functionDependencies = new ArrayList<>();
        ExpressionUtils.collectDependencies(normBody, rewriter, datasetDependencies, synonymDependencies,
                functionDependencies);

        List<Triple<DataverseName, String, String>> typeDependencies = Collections.emptyList();
        return ViewDetails.createDependencies(datasetDependencies, functionDependencies, typeDependencies,
                synonymDependencies);
    }
}
