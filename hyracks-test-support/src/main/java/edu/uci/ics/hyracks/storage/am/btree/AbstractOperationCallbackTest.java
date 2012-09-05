package edu.uci.ics.hyracks.storage.am.btree;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public abstract class AbstractOperationCallbackTest {
    protected static final int NUM_KEY_FIELDS = 1;

    @SuppressWarnings("rawtypes")
    protected final ISerializerDeserializer[] keySerdes;
    protected final MultiComparator cmp;

    protected IIndex index;

    protected abstract void createIndexInstance() throws Exception;

    public AbstractOperationCallbackTest() {
        this.keySerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };
        this.cmp = MultiComparator.create(SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length));
    }

    public void setup() throws Exception {
        createIndexInstance();
        index.create();
        index.activate();
    }

    public void tearDown() throws Exception {
        index.deactivate();
        index.destroy();
    }
}
