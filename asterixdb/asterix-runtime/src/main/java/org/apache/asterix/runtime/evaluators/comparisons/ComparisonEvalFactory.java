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
package org.apache.asterix.runtime.evaluators.comparisons;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class ComparisonEvalFactory implements IScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory evalLeftFactory;
    private IScalarEvaluatorFactory evalRightFactory;
    private ComparisonKind comparisonKind;

    public ComparisonEvalFactory(IScalarEvaluatorFactory evalLeftFactory, IScalarEvaluatorFactory evalRightFactory,
            ComparisonKind comparisonKind) {
        this.evalLeftFactory = evalLeftFactory;
        this.evalRightFactory = evalRightFactory;
        this.comparisonKind = comparisonKind;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        switch (comparisonKind) {
            // Should we do any normalization?
            case EQ: {
                return new EqualityComparisonEvaluator(evalLeftFactory, evalRightFactory, ctx);
            }
            case GE: {
                return new GreaterThanOrEqualComparisonEvaluator(evalLeftFactory, evalRightFactory, ctx);
            }
            case GT: {
                return new GreaterThanComparisonEvaluator(evalLeftFactory, evalRightFactory, ctx);
            }
            case LE: {
                return new LessThanOrEqualComparisonEvaluator(evalLeftFactory, evalRightFactory, ctx);
            }
            case LT: {
                return new LessThanComparisonEvaluator(evalLeftFactory, evalRightFactory, ctx);
            }
            case NEQ: {
                return new InequalityComparisonEvaluator(evalLeftFactory, evalRightFactory, ctx);
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    static class EqualityComparisonEvaluator extends AbstractComparisonEvaluator {
        public EqualityComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        protected boolean getComparisonResult(ComparisonResult r) {
            return (r == ComparisonResult.EQUAL);
        }

        @Override
        protected boolean isTotallyOrderable() {
            return false;
        }
    }

    static class InequalityComparisonEvaluator extends AbstractComparisonEvaluator {
        public InequalityComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        protected boolean getComparisonResult(ComparisonResult r) {
            return (r != ComparisonResult.EQUAL);
        }

        @Override
        protected boolean isTotallyOrderable() {
            return false;
        }
    }

    static class GreaterThanOrEqualComparisonEvaluator extends AbstractComparisonEvaluator {
        public GreaterThanOrEqualComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        protected boolean getComparisonResult(ComparisonResult r) {
            return (r == ComparisonResult.EQUAL || r == ComparisonResult.GREATER_THAN);
        }

        @Override
        protected boolean isTotallyOrderable() {
            return true;
        }
    }

    static class GreaterThanComparisonEvaluator extends AbstractComparisonEvaluator {
        public GreaterThanComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        protected boolean getComparisonResult(ComparisonResult r) {
            return (r == ComparisonResult.GREATER_THAN);
        }

        @Override
        protected boolean isTotallyOrderable() {
            return true;
        }
    }

    static class LessThanOrEqualComparisonEvaluator extends AbstractComparisonEvaluator {
        public LessThanOrEqualComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        protected boolean getComparisonResult(ComparisonResult r) {
            return (r == ComparisonResult.EQUAL || r == ComparisonResult.LESS_THAN);
        }

        @Override
        protected boolean isTotallyOrderable() {
            return true;
        }
    }

    static class LessThanComparisonEvaluator extends AbstractComparisonEvaluator {
        public LessThanComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        protected boolean getComparisonResult(ComparisonResult r) {
            return (r == ComparisonResult.LESS_THAN);
        }

        @Override
        protected boolean isTotallyOrderable() {
            return true;
        }
    }

}
