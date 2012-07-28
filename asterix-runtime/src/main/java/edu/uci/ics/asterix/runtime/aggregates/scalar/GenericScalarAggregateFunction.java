package edu.uci.ics.asterix.runtime.aggregates.scalar;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.aggregates.base.SingleFieldFrameTupleReference;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Implements scalar aggregates via the corresponding
 * ICopyAggregateFunction which should be passed inside this function.
 */
public class GenericScalarAggregateFunction implements ICopyEvaluator {

    private final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
    private final ICopyEvaluator argEval;
    private final ICopyAggregateFunction aggFunc;

    private final SingleFieldFrameTupleReference itemTuple = new SingleFieldFrameTupleReference();
    
    public GenericScalarAggregateFunction(ICopyEvaluatorFactory[] args, ICopyAggregateFunction aggFunc) throws AlgebricksException {
        this.argEval = args[0].createEvaluator(argOut);
        this.aggFunc = aggFunc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        argOut.reset();
        argEval.evaluate(tuple);
        byte[] listBytes = argOut.getByteArray();
        ATypeTag argType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[0]);        
        switch (argType) {
            case UNORDEREDLIST: {
                break;
            }
            case ORDEREDLIST: {
                try {
                    aggFunc.init();
                    ATypeTag itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[1]);
                    int numItems = AOrderedListSerializerDeserializer.getNumberOfItems(listBytes, 0);
                    for (int i = 0; i < numItems; i++) {
                        int itemOffset = AOrderedListSerializerDeserializer.getItemOffset(listBytes, i);
                        int itemLength = NonTaggedFormatUtil.getFieldValueLength(listBytes, itemOffset, itemTag, false);
                        itemTuple.reset(listBytes, itemOffset, itemLength);
                        aggFunc.step(itemTuple);
                    }
                    aggFunc.finish();
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
                break;
            }
            default: {
                throw new AlgebricksException("Unsupported type '" + argType + "' as input to scalar aggregate function.");
            }
        }        
    }
}
