package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class MinMaxAggregateFunction implements ICopyAggregateFunction {
    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
    private DataOutput out;
    private ICopyEvaluator eval;
    private ATypeTag aggType;
    private IBinaryComparator cmp;
    private final boolean isMin;
    private final boolean isLocalAgg;

    public MinMaxAggregateFunction(ICopyEvaluatorFactory[] args, IDataOutputProvider provider, boolean isMin,
            boolean isLocalAgg) throws AlgebricksException {
        out = provider.getDataOutput();
        eval = args[0].createEvaluator(inputVal);
        this.isMin = isMin;
        this.isLocalAgg = isLocalAgg;
    }

    @Override
    public void init() {
        aggType = ATypeTag.SYSTEM_NULL;
        outputVal.reset();
    }

    @Override
    public void step(IFrameTupleReference tuple) throws AlgebricksException {
        inputVal.reset();
        eval.evaluate(tuple);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[0]);
        if (typeTag == ATypeTag.NULL || aggType == ATypeTag.NULL) {
            aggType = ATypeTag.NULL;
            return;
        }
        if (aggType == ATypeTag.SYSTEM_NULL) {
            if (typeTag == ATypeTag.SYSTEM_NULL) {
                // Ignore.
                return;
            }
            // First value encountered. Set type, comparator, and initial value.
            aggType = typeTag;
            // Set comparator.
            IBinaryComparatorFactory cmpFactory = AqlBinaryComparatorFactoryProvider.INSTANCE
                    .getBinaryComparatorFactory(aggType, isMin);
            cmp = cmpFactory.createBinaryComparator();
            // Initialize min value.
            outputVal.assign(inputVal);
        } else if (typeTag != ATypeTag.SYSTEM_NULL && typeTag != aggType) {
            throw new AlgebricksException("Unexpected type " + typeTag + " in aggregation input stream. Expected type "
                    + aggType + ".");
        }
        if (cmp.compare(inputVal.getByteArray(), inputVal.getStartOffset(), inputVal.getLength(),
                outputVal.getByteArray(), outputVal.getStartOffset(), outputVal.getLength()) < 0) {
            outputVal.assign(inputVal);
        }
    }

    @Override
    public void finish() throws AlgebricksException {
        try {
            switch (aggType) {
                case NULL: {
                    out.writeByte(ATypeTag.NULL.serialize());
                    break;
                }
                case SYSTEM_NULL: {
                    // Empty stream. For local agg return system null. For global agg return null.
                    if (isLocalAgg) {
                        out.writeByte(ATypeTag.SYSTEM_NULL.serialize());
                    } else {
                        out.writeByte(ATypeTag.NULL.serialize());
                    }
                    break;
                }
                default: {
                    out.write(outputVal.getByteArray(), outputVal.getStartOffset(), outputVal.getLength());
                    break;
                }
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finishPartial() throws AlgebricksException {
        finish();
    }
}
