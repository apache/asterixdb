/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.comparisons;

import java.io.DataOutput;

import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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
            ComparisonResult r = compareResults();
            if (r == ComparisonResult.UNKNOWN) {
                try {
                    nullSerde.serialize(ANull.NULL, out);
                    return;
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            ABoolean b = (r == ComparisonResult.EQUAL) ? ABoolean.TRUE : ABoolean.FALSE;
            try {
                serde.serialize(b, out);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
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
            ComparisonResult r = compareResults();
            if (r == ComparisonResult.UNKNOWN) {
                try {
                    nullSerde.serialize(ANull.NULL, out);
                    return;
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            ABoolean b = (r != ComparisonResult.EQUAL) ? ABoolean.TRUE : ABoolean.FALSE;
            try {
                serde.serialize(b, out);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
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
            checkComparable();
            ComparisonResult r = compareResults();
            if (r == ComparisonResult.UNKNOWN) {
                try {
                    nullSerde.serialize(ANull.NULL, out);
                    return;
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            ABoolean b = (r == ComparisonResult.EQUAL || r == ComparisonResult.GREATER_THAN) ? ABoolean.TRUE
                    : ABoolean.FALSE;
            try {
                serde.serialize(b, out);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
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
            checkComparable();
            ComparisonResult r = compareResults();
            if (r == ComparisonResult.UNKNOWN) {
                try {
                    nullSerde.serialize(ANull.NULL, out);
                    return;
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            ABoolean b = (r == ComparisonResult.GREATER_THAN) ? ABoolean.TRUE : ABoolean.FALSE;
            try {
                serde.serialize(b, out);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
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
            checkComparable();
            ComparisonResult r = compareResults();
            if (r == ComparisonResult.UNKNOWN) {
                try {
                    nullSerde.serialize(ANull.NULL, out);
                    return;
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            ABoolean b = (r == ComparisonResult.EQUAL || r == ComparisonResult.LESS_THAN) ? ABoolean.TRUE
                    : ABoolean.FALSE;
            try {
                serde.serialize(b, out);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
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
            checkComparable();
            ComparisonResult r = compareResults();
            if (r == ComparisonResult.UNKNOWN) {
                try {
                    nullSerde.serialize(ANull.NULL, out);
                    return;
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            ABoolean b = (r == ComparisonResult.LESS_THAN) ? ABoolean.TRUE : ABoolean.FALSE;
            try {
                serde.serialize(b, out);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
        }

    }

}