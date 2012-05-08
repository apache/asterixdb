package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AInt32AscNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AInt32DescNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AStringAscNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AStringDescNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class AqlNormalizedKeyComputerFactoryProvider implements INormalizedKeyComputerFactoryProvider {

    public static final AqlNormalizedKeyComputerFactoryProvider INSTANCE = new AqlNormalizedKeyComputerFactoryProvider();

    private AqlNormalizedKeyComputerFactoryProvider() {
    }

    @Override
    public INormalizedKeyComputerFactory getNormalizedKeyComputerFactory(Object type, boolean ascending) {
        IAType aqlType = (IAType) type;
        if (ascending) {
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
        } else {
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
        }
    }

}
