package edu.uci.ics.asterix.runtime.aggregates.scalar;

import edu.uci.ics.asterix.runtime.aggregates.base.SingleFieldFrameTupleReference;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Implements scalar aggregates by iterating over a collection with the ScanCollection unnesting function,
 * and applying the corresponding ICopyAggregateFunction to each collection-item.
 */
public class GenericScalarAggregateFunction implements ICopyEvaluator {

    private final ArrayBackedValueStorage listItemOut = new ArrayBackedValueStorage();
    private final ICopyAggregateFunction aggFunc;
    private final ICopyUnnestingFunction scanCollection;

    private final SingleFieldFrameTupleReference itemTuple = new SingleFieldFrameTupleReference();

    public GenericScalarAggregateFunction(ICopyAggregateFunction aggFunc,
            ICopyUnnestingFunctionFactory scanCollectionFactory) throws AlgebricksException {
        this.aggFunc = aggFunc;
        this.scanCollection = scanCollectionFactory.createUnnestingFunction(listItemOut);
        listItemOut.reset();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        scanCollection.init(tuple);
        aggFunc.init();
        while (scanCollection.step()) {
            itemTuple.reset(listItemOut.getByteArray(), 0, listItemOut.getLength());
            aggFunc.step(itemTuple);
            listItemOut.reset();
        }
        aggFunc.finish();
    }
}
