package edu.uci.ics.hivesterix.runtime.inspector;

import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class HiveBinaryBooleanInspectorFactory implements IBinaryBooleanInspectorFactory {
    private static final long serialVersionUID = 1L;
    public static HiveBinaryBooleanInspectorFactory INSTANCE = new HiveBinaryBooleanInspectorFactory();

    private HiveBinaryBooleanInspectorFactory() {

    }

    @Override
    public IBinaryBooleanInspector createBinaryBooleanInspector(IHyracksTaskContext arg0) {
        return new HiveBinaryBooleanInspector();
    }

}
