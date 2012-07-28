package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CountAggregateFunction implements ICopyAggregateFunction {
    private AMutableInt32 result = new AMutableInt32(-1);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    private int cnt;
    private DataOutput out;
    
    public CountAggregateFunction(IDataOutputProvider output) {
        out = output.getDataOutput();
    }
    
    @Override
    public void init() {
        cnt = 0;
    }

    @Override
    public void step(IFrameTupleReference tuple) throws AlgebricksException {
        cnt++;
    }

    @Override
    public void finish() throws AlgebricksException {
        try {
            result.setValue(cnt);
            int32Serde.serialize(result, out);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finishPartial() throws AlgebricksException {
        finish();
    }
}
