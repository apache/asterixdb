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
package org.apache.asterix.lang.common.base;

import java.util.Collection;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Contract for all extension AST nodes to allow existing rewrites to support language extensions.
 */
public interface IVisitorExtension {
    Expression simpleExpressionDispatch(ILangVisitor<Expression, ILangExpression> simpleExpressionVisitor,
            ILangExpression argument) throws CompilationException;

    Void freeVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> freeVariableVisitor,
            Collection<VariableExpr> freeVariables) throws CompilationException;

    Void bindingVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> bindingVariableVisitor,
            Collection<VariableExpr> bindingVariables) throws CompilationException;

    Expression variableScopeDispatch(ILangVisitor<Expression, ILangExpression> scopingVisitor, ILangExpression argument,
            ScopeChecker scopeChecker) throws CompilationException;

    ILangExpression deepCopyDispatch(ILangVisitor<ILangExpression, Void> deepCopyVisitor) throws CompilationException;

    Pair<ILangExpression, VariableSubstitutionEnvironment> remapCloneDispatch(
            ILangVisitor<Pair<ILangExpression, VariableSubstitutionEnvironment>, VariableSubstitutionEnvironment> remapCloneVisitor,
            VariableSubstitutionEnvironment substitutionEnvironment) throws CompilationException;

    Boolean inlineUDFsDispatch(ILangVisitor<Boolean, Void> inlineUDFsVisitor) throws CompilationException;

    Void gatherFunctionsDispatch(ILangVisitor<Void, Void> gatherFunctionsVisitor,
            Collection<? super AbstractCallExpression> functionCalls) throws CompilationException;

    Boolean checkSubqueryDispatch(ILangVisitor<Boolean, ILangExpression> checkSubqueryVisitor, ILangExpression argument)
            throws CompilationException;

    Boolean check92AggregateDispatch(ILangVisitor<Boolean, ILangExpression> check92AggregateVisitor,
            ILangExpression argument) throws CompilationException;

    Boolean checkNonFunctionalDispatch(ILangVisitor<Boolean, Void> checkNonFunctionalVisitor)
            throws CompilationException;

    Boolean checkDatasetOnlyDispatch(ILangVisitor<Boolean, VariableExpr> checkDatasetOnlyVisitor,
            VariableExpr datasetCandidate) throws CompilationException;
}
