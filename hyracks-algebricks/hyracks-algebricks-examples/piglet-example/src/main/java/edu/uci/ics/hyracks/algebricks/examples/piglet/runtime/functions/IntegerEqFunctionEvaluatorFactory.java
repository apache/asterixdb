package edu.uci.ics.hyracks.algebricks.examples.piglet.runtime.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class IntegerEqFunctionEvaluatorFactory implements IEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private final IEvaluatorFactory arg1Factory;

    private final IEvaluatorFactory arg2Factory;

    public IntegerEqFunctionEvaluatorFactory(IEvaluatorFactory arg1Factory, IEvaluatorFactory arg2Factory) {
        this.arg1Factory = arg1Factory;
        this.arg2Factory = arg2Factory;
    }

    @Override
    public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new IEvaluator() {
            private DataOutput dataout = output.getDataOutput();
            private ArrayBackedValueStorage out1 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage out2 = new ArrayBackedValueStorage();
            private IEvaluator eval1 = arg1Factory.createEvaluator(out1);
            private IEvaluator eval2 = arg2Factory.createEvaluator(out2);

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                out1.reset();
                eval1.evaluate(tuple);
                out2.reset();
                eval2.evaluate(tuple);
                int v1 = IntegerSerializerDeserializer.getInt(out1.getBytes(), 0);
                int v2 = IntegerSerializerDeserializer.getInt(out2.getBytes(), 0);
                boolean r = v1 == v2;
                try {
                    dataout.writeBoolean(r);
                } catch (IOException ioe) {
                    throw new AlgebricksException(ioe);
                }
            }
        };
    }
}