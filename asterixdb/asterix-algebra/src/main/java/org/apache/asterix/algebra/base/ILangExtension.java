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

package org.apache.asterix.algebra.base;

import org.apache.asterix.common.api.IExtension;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

/**
 * An interface for language extensions
 */
public interface ILangExtension extends IExtension {

    public enum Language {
        AQL,
        SQLPP
    }

    @Override
    default ExtensionKind getExtensionKind() {
        return ExtensionKind.LANG;
    }

    ILangCompilationProvider getLangCompilationProvider(Language lang);

    //TODO(amoudi/yingyi) this is not a good way to extend re-write rules. introduce rewrite-rule-provider.
    /**
     * Called by the compiler when the unnest function is an extension function.
     * Provides a way to add additional types of datasources
     *
     * @param opRef
     * @param context
     * @param unnestOp
     * @param unnestExpr
     * @param functionCallExpr
     * @return true if transformation was successful, false otherwise
     * @throws AlgebricksException
     */
    boolean unnestToDataScan(Mutable<ILogicalOperator> opRef, IOptimizationContext context, UnnestOperator unnestOp,
            ILogicalExpression unnestExpr, AbstractFunctionCallExpression functionCallExpr) throws AlgebricksException;
}
