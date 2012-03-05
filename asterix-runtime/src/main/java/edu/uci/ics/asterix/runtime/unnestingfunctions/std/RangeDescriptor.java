package edu.uci.ics.asterix.runtime.unnestingfunctions.std;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class RangeDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "range", 2, true);

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public IUnnestingFunctionFactory createUnnestingFunctionFactory(final IEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IUnnestingFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IUnnestingFunction createUnnestingFunction(final IDataOutputProvider provider)
                    throws AlgebricksException {
                return new IUnnestingFunction() {

                    private DataOutput out = provider.getDataOutput();
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private IEvaluator eval0 = args[0].createEvaluator(inputVal);
                    private IEvaluator eval1 = args[1].createEvaluator(inputVal);
                    private AMutableInt32 aInt32 = new AMutableInt32(0);
                    private int current;
                    private int max;

                    @Override
                    public void init(IFrameTupleReference tuple) throws AlgebricksException {
                        inputVal.reset();
                        eval0.evaluate(tuple);
                        current = IntegerSerializerDeserializer.getInt(inputVal.getBytes(), 1);
                        inputVal.reset();
                        eval1.evaluate(tuple);
                        max = IntegerSerializerDeserializer.getInt(inputVal.getBytes(), 1);
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean step() throws AlgebricksException {
                        if (current > max) {
                            return false;
                        }
                        aInt32.setValue(current);
                        try {
                            serde.serialize(aInt32, out);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                        current++;
                        return true;
                    }

                };
            }
        };
    }

}
