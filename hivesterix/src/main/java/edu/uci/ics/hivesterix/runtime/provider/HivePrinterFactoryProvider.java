package edu.uci.ics.hivesterix.runtime.provider;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;

public class HivePrinterFactoryProvider implements IPrinterFactoryProvider {

	public static IPrinterFactoryProvider INSTANCE = new HivePrinterFactoryProvider();

	@Override
	public IPrinterFactory getPrinterFactory(Object type)
			throws AlgebricksException {
		return null;
	}

}
