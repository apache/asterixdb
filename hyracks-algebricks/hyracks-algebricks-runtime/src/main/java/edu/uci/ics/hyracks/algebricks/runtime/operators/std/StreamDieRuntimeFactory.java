package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.context.RuntimeContext;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class StreamDieRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory aftterObjectsEvalFactory;
    private IBinaryIntegerInspector binaryIntegerInspector;

    public StreamDieRuntimeFactory(ICopyEvaluatorFactory maxObjectsEvalFactory, int[] projectionList,
            IBinaryIntegerInspector binaryIntegerInspector) {
        super(projectionList);
        this.aftterObjectsEvalFactory = maxObjectsEvalFactory;
        this.binaryIntegerInspector = binaryIntegerInspector;
    }

    @Override
    public String toString() {
        String s = "stream-die " + aftterObjectsEvalFactory.toString();
        return s;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final RuntimeContext context) {
        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private ICopyEvaluator evalAfterObjects;
            private ArrayBackedValueStorage evalOutput;
            private int toWrite = -1;

            @Override
            public void open() throws HyracksDataException {
                if (evalAfterObjects == null) {
                    initAccessAppendRef(context);
                    evalOutput = new ArrayBackedValueStorage();
                    try {
                        evalAfterObjects = aftterObjectsEvalFactory.createEvaluator(evalOutput);
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                if (toWrite < 0) {
                    toWrite = evaluateInteger(evalAfterObjects, 0);
                }
                for (int t = 0; t < nTuple; t++) {
                    if (toWrite > 0) {
                        toWrite--;
                        if (projectionList != null) {
                            appendProjectionToFrame(t, projectionList);
                        } else {
                            appendTupleToFrame(t);
                        }
                    } else {
                        throw new HyracksDataException("injected failure");
                    }
                }
            }

            @Override
            public void close() throws HyracksDataException {
                super.close();
            }

            private int evaluateInteger(ICopyEvaluator eval, int tIdx) throws HyracksDataException {
                tRef.reset(tAccess, tIdx);
                evalOutput.reset();
                try {
                    eval.evaluate(tRef);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
                int lim = binaryIntegerInspector.getIntegerValue(evalOutput.getByteArray(), 0, evalOutput.getLength());
                return lim;
            }

        };
    }

}
