package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.context.RuntimeContext;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class StreamLimitRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory maxObjectsEvalFactory;
    private IScalarEvaluatorFactory offsetEvalFactory;
    private IBinaryIntegerInspector binaryIntegerInspector;

    public StreamLimitRuntimeFactory(IScalarEvaluatorFactory maxObjectsEvalFactory,
            IScalarEvaluatorFactory offsetEvalFactory, int[] projectionList,
            IBinaryIntegerInspector binaryIntegerInspector) {
        super(projectionList);
        this.maxObjectsEvalFactory = maxObjectsEvalFactory;
        this.offsetEvalFactory = offsetEvalFactory;
        this.binaryIntegerInspector = binaryIntegerInspector;
    }

    @Override
    public String toString() {
        String s = "stream-limit " + maxObjectsEvalFactory.toString();
        if (offsetEvalFactory != null) {
            return s + ", " + offsetEvalFactory.toString();
        } else {
            return s;
        }
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final RuntimeContext context) {
        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private IScalarEvaluator evalMaxObjects;
            private IScalarEvaluator evalOffset = null;
            private int toWrite = 0; // how many tuples still to write
            private int toSkip = 0; // how many tuples still to skip
            private boolean firstTuple = true;
            private boolean afterLastTuple = false;

            @Override
            public void open() throws HyracksDataException {
                // if (first) {
                if (evalMaxObjects == null) {
                    initAccessAppendRef(context);
                    try {
                        evalMaxObjects = maxObjectsEvalFactory.createScalarEvaluator();
                        if (offsetEvalFactory != null) {
                            evalOffset = offsetEvalFactory.createScalarEvaluator();
                        }
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
                writer.open();
                afterLastTuple = false;
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (afterLastTuple) {
                    // ignore the data
                    return;
                }
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                int start = 0;
                if (nTuple <= toSkip) {
                    toSkip -= nTuple;
                    return;
                } else if (toSkip > 0) {
                    start = toSkip;
                    toSkip = 0;
                }
                for (int t = start; t < nTuple; t++) {
                    if (firstTuple) {
                        firstTuple = false;
                        toWrite = evaluateInteger(evalMaxObjects, t);
                        if (evalOffset != null) {
                            toSkip = evaluateInteger(evalOffset, t);
                        }
                    }
                    if (toSkip > 0) {
                        toSkip--;
                    } else if (toWrite > 0) {
                        toWrite--;
                        if (projectionList != null) {
                            appendProjectionToFrame(t, projectionList);
                        } else {
                            appendTupleToFrame(t);
                        }
                    } else {
                        // close();
                        afterLastTuple = true;
                        break;
                    }
                }
            }

            @Override
            public void close() throws HyracksDataException {
                // if (!afterLastTuple) {
                super.close();
                // }
            }

            private int evaluateInteger(IScalarEvaluator eval, int tIdx) throws HyracksDataException {
                tRef.reset(tAccess, tIdx);
                try {
                    eval.evaluate(tRef, p);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
                int lim = binaryIntegerInspector.getIntegerValue(p.getByteArray(), p.getStartOffset(), p.getLength());
                return lim;
            }

        };
    }

}
