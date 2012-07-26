package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractStringContainsEval implements ICopyEvaluator {

    private DataOutput dout;

    private ArrayBackedValueStorage array0 = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage array1 = new ArrayBackedValueStorage();
    private ICopyEvaluator evalString;
    private ICopyEvaluator evalPattern;
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer boolSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);

    public AbstractStringContainsEval(DataOutput dout, ICopyEvaluatorFactory evalStringFactory,
            ICopyEvaluatorFactory evalPatternFactory) throws AlgebricksException {
        this.dout = dout;
        this.evalString = evalStringFactory.createEvaluator(array0);
        this.evalPattern = evalPatternFactory.createEvaluator(array1);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        array1.reset();
        evalPattern.evaluate(tuple);
        array0.reset();
        evalString.evaluate(tuple);
        byte[] b1 = array0.getByteArray();
        byte[] b2 = array1.getByteArray();
        ABoolean res = findMatch(b1, b2) ? ABoolean.TRUE : ABoolean.FALSE;
        try {
            boolSerde.serialize(res, dout);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    protected abstract boolean findMatch(byte[] strBytes, byte[] patternBytes);

}
