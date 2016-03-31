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

import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
            resultStorage.reset();
            evalInputs(tuple);
            // Checks whether two types are comparable
            switch (comparabilityCheck()) {
                case UNKNOWN:
                    // result:UNKNOWN - NULL value found
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                        result.set(resultStorage);
                        return;
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                case FALSE:
                    // result:FALSE - two types cannot be compared. Thus we return FALSE since this is equality comparison
                    ABoolean b = ABoolean.FALSE;
                    try {
                        serde.serialize(b, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                case TRUE:
                    // Two types can be compared
                    ComparisonResult r = compareResults();
                    ABoolean b1 = (r == ComparisonResult.EQUAL) ? ABoolean.TRUE : ABoolean.FALSE;
                    try {
                        serde.serialize(b1, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                default:
                    throw new AlgebricksException(
                            "Equality Comparison cannot be processed. The return code from ComparabilityCheck is not correct.");
            }
            result.set(resultStorage);
        }

    }

    static class InequalityComparisonEvaluator extends AbstractComparisonEvaluator {
        public InequalityComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
            resultStorage.reset();
            evalInputs(tuple);

            // Checks whether two types are comparable
            switch (comparabilityCheck()) {
                case UNKNOWN:
                    // result:UNKNOWN - NULL value found
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                        result.set(resultStorage);
                        return;
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                case FALSE:
                    // result:FALSE - two types cannot be compared. Thus we return TRUE since this is NOT EQ comparison.
                    ABoolean b = ABoolean.TRUE;
                    try {
                        serde.serialize(b, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                case TRUE:
                    // Two types can be compared
                    ComparisonResult r = compareResults();
                    ABoolean b1 = (r != ComparisonResult.EQUAL) ? ABoolean.TRUE : ABoolean.FALSE;
                    try {
                        serde.serialize(b1, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                default:
                    throw new AlgebricksException(
                            "Inequality Comparison cannot be processed. The return code from ComparabilityCheck is not correct.");
            }
            result.set(resultStorage);
        }

    }

    static class GreaterThanOrEqualComparisonEvaluator extends AbstractComparisonEvaluator {
        public GreaterThanOrEqualComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
            resultStorage.reset();
            evalInputs(tuple);

            // checks whether we can apply >, >=, <, and <= to the given type since
            // these operations cannot be defined for certain types.
            checkTotallyOrderable();

            // Checks whether two types are comparable
            switch (comparabilityCheck()) {
                case UNKNOWN:
                    // result:UNKNOWN - NULL value found
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                        result.set(resultStorage);
                        return;
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                case FALSE:
                    // result:FALSE - two types cannot be compared. Thus we return FALSE since this is an inequality comparison.
                    ABoolean b = ABoolean.FALSE;
                    try {
                        serde.serialize(b, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                case TRUE:
                    // Two types can be compared
                    ComparisonResult r = compareResults();
                    ABoolean b1 = (r == ComparisonResult.EQUAL || r == ComparisonResult.GREATER_THAN) ? ABoolean.TRUE
                            : ABoolean.FALSE;
                    try {
                        serde.serialize(b1, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                default:
                    throw new AlgebricksException(
                            "Inequality Comparison cannot be processed. The return code from ComparabilityCheck is not correct.");
            }
            result.set(resultStorage);
        }

    }

    static class GreaterThanComparisonEvaluator extends AbstractComparisonEvaluator {
        public GreaterThanComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
            resultStorage.reset();
            evalInputs(tuple);

            // checks whether we can apply >, >=, <, and <= to the given type since
            // these operations cannot be defined for certain types.
            checkTotallyOrderable();

            // Checks whether two types are comparable
            switch (comparabilityCheck()) {
                case UNKNOWN:
                    // result:UNKNOWN - NULL value found
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                        result.set(resultStorage);
                        return;
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                case FALSE:
                    // result:FALSE - two types cannot be compared. Thus we return FALSE since this is an inequality comparison.
                    ABoolean b = ABoolean.FALSE;
                    try {
                        serde.serialize(b, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                case TRUE:
                    // Two types can be compared
                    ComparisonResult r = compareResults();
                    ABoolean b1 = (r == ComparisonResult.GREATER_THAN) ? ABoolean.TRUE : ABoolean.FALSE;
                    try {
                        serde.serialize(b1, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                default:
                    throw new AlgebricksException(
                            "Inequality Comparison cannot be processed. The return code from ComparabilityCheck is not correct.");
            }
            result.set(resultStorage);
        }

    }

    static class LessThanOrEqualComparisonEvaluator extends AbstractComparisonEvaluator {
        public LessThanOrEqualComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
            resultStorage.reset();
            evalInputs(tuple);

            // checks whether we can apply >, >=, <, and <= to the given type since
            // these operations cannot be defined for certain types.
            checkTotallyOrderable();

            // Checks whether two types are comparable
            switch (comparabilityCheck()) {
                case UNKNOWN:
                    // result:UNKNOWN - NULL value found
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                        result.set(resultStorage);
                        return;
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                case FALSE:
                    // result:FALSE - two types cannot be compared. Thus we return FALSE since this is an inequality comparison.
                    ABoolean b = ABoolean.FALSE;
                    try {
                        serde.serialize(b, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                case TRUE:
                    // Two types can be compared
                    ComparisonResult r = compareResults();
                    ABoolean b1 = (r == ComparisonResult.EQUAL || r == ComparisonResult.LESS_THAN) ? ABoolean.TRUE
                            : ABoolean.FALSE;
                    try {
                        serde.serialize(b1, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                default:
                    throw new AlgebricksException(
                            "Inequality Comparison cannot be processed. The return code from ComparabilityCheck is not correct.");
            }
            result.set(resultStorage);
        }

    }

    static class LessThanComparisonEvaluator extends AbstractComparisonEvaluator {
        public LessThanComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
            super(evalLeftFactory, evalRightFactory, context);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
            resultStorage.reset();
            evalInputs(tuple);

            // checks whether we can apply >, >=, <, and <= to the given type since
            // these operations cannot be defined for certain types.
            checkTotallyOrderable();

            // Checks whether two types are comparable
            switch (comparabilityCheck()) {
                case UNKNOWN:
                    // result:UNKNOWN - NULL value found
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                        result.set(resultStorage);
                        return;
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                case FALSE:
                    // result:FALSE - two types cannot be compared. Thus we return FALSE since this is an inequality comparison.
                    ABoolean b = ABoolean.FALSE;
                    try {
                        serde.serialize(b, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                case TRUE:
                    // Two types can be compared
                    ComparisonResult r = compareResults();
                    ABoolean b1 = (r == ComparisonResult.LESS_THAN) ? ABoolean.TRUE : ABoolean.FALSE;
                    try {
                        serde.serialize(b1, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    break;
                default:
                    throw new AlgebricksException(
                            "Inequality Comparison cannot be processed. The return code from ComparabilityCheck is not correct.");
            }
            result.set(resultStorage);
        }

    }

}
