package edu.uci.ics.hivesterix.runtime.inspector;

import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;

public class HiveBinaryBooleanInspector implements IBinaryBooleanInspector {

	HiveBinaryBooleanInspector() {
	}

	@Override
	public boolean getBooleanValue(byte[] bytes, int offset, int length) {
		if (length == 0)
			return false;
		if (length != 1)
			throw new IllegalStateException("boolean field error: with length "
					+ length);
		return bytes[0] == 1;
	}

}
