package edu.uci.ics.asterix.runtime.aggregates.serializable.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.aggregates.base.IAccumulator;
import edu.uci.ics.asterix.runtime.aggregates.base.SumAccumulator;
import edu.uci.ics.asterix.runtime.aggregates.base.WrappingMutableValueStorage;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunction;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SerializableSumAggregateFunction implements ICopySerializableAggregateFunction {
    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage defaultVal = new ArrayBackedValueStorage();
    private WrappingMutableValueStorage stateWrapper = new WrappingMutableValueStorage();
    private ICopyEvaluator eval;
    private IAccumulator accumulator = new SumAccumulator();
    
    public SerializableSumAggregateFunction(ICopyEvaluatorFactory[] args, boolean isLocalAgg)
            throws AlgebricksException {
        eval = args[0].createEvaluator(inputVal);
        try {
            if (isLocalAgg) {
                defaultVal.getDataOutput().writeByte(ATypeTag.SYSTEM_NULL.serialize());
            } else {
                defaultVal.getDataOutput().writeByte(ATypeTag.NULL.serialize());
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
    
    @Override
    public void init(DataOutput state) throws AlgebricksException {
        try {            
            stateWrapper.wrap(state);
            accumulator.init(stateWrapper, defaultVal);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws AlgebricksException {
        inputVal.reset();
        eval.evaluate(tuple);
        stateWrapper.wrap(state, start, len);
        try {
            accumulator.step(stateWrapper, inputVal);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finish(byte[] state, int start, int len, DataOutput out) throws AlgebricksException {
        try {
            stateWrapper.wrap(state, start, len);
            accumulator.finish(stateWrapper, out);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finishPartial(byte[] state, int start, int len, DataOutput out)
            throws AlgebricksException {
        finish(state, start, len, out);
    }
}
