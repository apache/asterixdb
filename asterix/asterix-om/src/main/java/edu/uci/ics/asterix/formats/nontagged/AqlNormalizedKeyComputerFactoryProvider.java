package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AInt32AscNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AInt32DescNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AStringAscNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AStringDescNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class AqlNormalizedKeyComputerFactoryProvider implements INormalizedKeyComputerFactoryProvider {

    public static final AqlNormalizedKeyComputerFactoryProvider INSTANCE = new AqlNormalizedKeyComputerFactoryProvider();

    private AqlNormalizedKeyComputerFactoryProvider() {
    }

    @Override
    public INormalizedKeyComputerFactory getNormalizedKeyComputerFactory(Object type, OrderKind order) {
        IAType aqlType = (IAType) type;
        if (order == OrderKind.ASC) {
            switch (aqlType.getTypeTag()) {
                case INT32: {
                    return AInt32AscNormalizedKeyComputerFactory.INSTANCE;
                }
                case STRING: {
                    return AStringAscNormalizedKeyComputerFactory.INSTANCE;
                }
                default: {
                    return null;
                }
            }
        } else if (order == OrderKind.DESC) {
            switch (aqlType.getTypeTag()) {
                case INT32: {
                    return AInt32DescNormalizedKeyComputerFactory.INSTANCE;
                }
                case STRING: {
                    return AStringDescNormalizedKeyComputerFactory.INSTANCE;
                }
                default: {
                    return null;
                }
            }
        } else
            return null;
    }

}
