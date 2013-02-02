package edu.uci.ics.hivesterix.runtime.provider;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveDoubleAscNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveDoubleDescNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveIntegerAscNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveIntegerDescNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveLongAscNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveLongDescNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveStringAscNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveStringDescNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class HiveNormalizedKeyComputerFactoryProvider implements
		INormalizedKeyComputerFactoryProvider {

	public static final HiveNormalizedKeyComputerFactoryProvider INSTANCE = new HiveNormalizedKeyComputerFactoryProvider();

	private HiveNormalizedKeyComputerFactoryProvider() {
	}

	@Override
	public INormalizedKeyComputerFactory getNormalizedKeyComputerFactory(
			Object type, boolean ascending) {
		if (ascending) {
			if (type.equals(TypeInfoFactory.stringTypeInfo)) {
				return new HiveStringAscNormalizedKeyComputerFactory();
			} else if (type.equals(TypeInfoFactory.intTypeInfo)) {
				return new HiveIntegerAscNormalizedKeyComputerFactory();
			} else if (type.equals(TypeInfoFactory.longTypeInfo)) {
				return new HiveLongAscNormalizedKeyComputerFactory();
			} else if (type.equals(TypeInfoFactory.doubleTypeInfo)) {
				return new HiveDoubleAscNormalizedKeyComputerFactory();
			} else {
				return null;
			}
		} else {
			if (type.equals(TypeInfoFactory.stringTypeInfo)) {
				return new HiveStringDescNormalizedKeyComputerFactory();
			} else if (type.equals(TypeInfoFactory.intTypeInfo)) {
				return new HiveIntegerDescNormalizedKeyComputerFactory();
			} else if (type.equals(TypeInfoFactory.longTypeInfo)) {
				return new HiveLongDescNormalizedKeyComputerFactory();
			} else if (type.equals(TypeInfoFactory.doubleTypeInfo)) {
				return new HiveDoubleDescNormalizedKeyComputerFactory();
			} else {
				return null;
			}
		}
	}
}
