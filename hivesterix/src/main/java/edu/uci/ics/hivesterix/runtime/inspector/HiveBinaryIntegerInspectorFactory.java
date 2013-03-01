package edu.uci.ics.hivesterix.runtime.inspector;

import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class HiveBinaryIntegerInspectorFactory implements
		IBinaryIntegerInspectorFactory {
	private static final long serialVersionUID = 1L;
	public static HiveBinaryIntegerInspectorFactory INSTANCE = new HiveBinaryIntegerInspectorFactory();

	private HiveBinaryIntegerInspectorFactory() {

	}

	@Override
	public IBinaryIntegerInspector createBinaryIntegerInspector(
			IHyracksTaskContext arg0) {
		return new HiveBinaryIntegerInspector();
	}

}
