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

package org.apache.asterix.optimizer.rules;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.optimizer.rules.util.InsertUpsertCheckUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule checks whether a returning clause for INSERT/UPSERT contains a dataset access.
 * NOTE: the rule should be invoked before rewriting Unnests (with dataset functions) into DataSourceScans.
 */
public class CheckInsertUpsertReturningRule implements IAlgebraicRewriteRule {

    private boolean checked = false;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (checked) {
            return false;
        }
        ILogicalOperator op = opRef.getValue();
        if (InsertUpsertCheckUtil.check(op)) {
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_RETURNING_EXPRESSION, op.getSourceLocation());
        }
        checked = true;
        return false;
    }
}
