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

import java.io.DataOutput;

import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ComparisonEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory evalLeftFactory;
    private ICopyEvaluatorFactory evalRightFactory;
    private ComparisonKind comparisonKind;

    public ComparisonEvalFactory(ICopyEvaluatorFactory evalLeftFactory, ICopyEvaluatorFactory evalRightFactory,
            ComparisonKind comparisonKind) {
        this.evalLeftFactory = evalLeftFactory;
        this.evalRightFactory = evalRightFactory;
        this.comparisonKind = comparisonKind;
    }

    @Override
    public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
        DataOutput out = output.getDataOutput();
        switch (comparisonKind) {
        // Should we do any normalization?
            case EQ: {
                return new EqualityComparisonEvaluator(out, evalLeftFactory, evalRightFactory);
            }
            case GE: {
                return new GreaterThanOrEqualComparisonEvaluator(out, evalLeftFactory, evalRightFactory);
            }
            case GT: {
                return new GreaterThanComparisonEvaluator(out, evalLeftFactory, evalRightFactory);
            }
            case LE: {
                return new LessThanOrEqualComparisonEvaluator(out, evalLeftFactory, evalRightFactory);
            }
            case LT: {
                return new LessThanComparisonEvaluator(out, evalLeftFactory, evalRightFactory);
            }
            case NEQ: {
                return new InequalityComparisonEvaluator(out, evalLeftFactory, evalRightFactory);
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    static class EqualityComparisonEvaluator extends AbstractComparisonEvaluator {
        public EqualityComparisonEvaluator(DataOutput out, ICopyEvaluatorFactory evalLeftFactory,
                ICopyEvaluatorFactory evalRightFactory) throws AlgebricksException {
            super(out, evalLeftFactory, evalRightFactory);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
            evalInputs(tuple);

            // Checks whether two types are comparable
            switch (comparabilityCheck()) {
                case UNKNOWN:
                    // result:UNKNOWN - NULL value found
                    try {
                        nullSerde.serialize(ANull.NULL, out);
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

        }

    }

    static class InequalityComparisonEvaluator extends AbstractComparisonEvaluator {
        public InequalityComparisonEvaluator(DataOutput out, ICopyEvaluatorFactory evalLeftFactory,
                ICopyEvaluatorFactory evalRightFactory) throws AlgebricksException {
            super(out, evalLeftFactory, evalRightFactory);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
            evalInputs(tuple);

            // Checks whether two types are comparable
            switch (comparabilityCheck()) {
                case UNKNOWN:
                    // result:UNKNOWN - NULL value found
                    try {
                        nullSerde.serialize(ANull.NULL, out);
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

        }

    }

    static class GreaterThanOrEqualComparisonEvaluator extends AbstractComparisonEvaluator {
        public GreaterThanOrEqualComparisonEvaluator(DataOutput out, ICopyEvaluatorFactory evalLeftFactory,
                ICopyEvaluatorFactory evalRightFactory) throws AlgebricksException {
            super(out, evalLeftFactory, evalRightFactory);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
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

        }

    }

    static class GreaterThanComparisonEvaluator extends AbstractComparisonEvaluator {
        public GreaterThanComparisonEvaluator(DataOutput out, ICopyEvaluatorFactory evalLeftFactory,
                ICopyEvaluatorFactory evalRightFactory) throws AlgebricksException {
            super(out, evalLeftFactory, evalRightFactory);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
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

        }

    }

    static class LessThanOrEqualComparisonEvaluator extends AbstractComparisonEvaluator {
        public LessThanOrEqualComparisonEvaluator(DataOutput out, ICopyEvaluatorFactory evalLeftFactory,
                ICopyEvaluatorFactory evalRightFactory) throws AlgebricksException {
            super(out, evalLeftFactory, evalRightFactory);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
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

        }

    }

    static class LessThanComparisonEvaluator extends AbstractComparisonEvaluator {
        public LessThanComparisonEvaluator(DataOutput out, ICopyEvaluatorFactory evalLeftFactory,
                ICopyEvaluatorFactory evalRightFactory) throws AlgebricksException {
            super(out, evalLeftFactory, evalRightFactory);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
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

        }

    }

}