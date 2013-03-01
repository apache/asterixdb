package edu.uci.ics.hivesterix.logical.expression;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;

public class HiveNullableTypeComputer implements INullableTypeComputer {

	public static INullableTypeComputer INSTANCE = new HiveNullableTypeComputer();

	@Override
	public Object makeNullableType(Object type) throws AlgebricksException {
		return type;
	}

}
