package edu.uci.ics.hivesterix.runtime.provider;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveDoubleBinaryHashFunctionFactory;
import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveIntegerBinaryHashFunctionFactory;
import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveLongBinaryHashFunctionFactory;
import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveRawBinaryHashFunctionFactory;
import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveStingBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class HiveBinaryHashFunctionFactoryProvider implements IBinaryHashFunctionFactoryProvider {

    public static final HiveBinaryHashFunctionFactoryProvider INSTANCE = new HiveBinaryHashFunctionFactoryProvider();

    private HiveBinaryHashFunctionFactoryProvider() {
    }

    @Override
    public IBinaryHashFunctionFactory getBinaryHashFunctionFactory(Object type) throws AlgebricksException {
        if (type.equals(TypeInfoFactory.intTypeInfo)) {
            return HiveIntegerBinaryHashFunctionFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.longTypeInfo)) {
            return HiveLongBinaryHashFunctionFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.stringTypeInfo)) {
            return HiveStingBinaryHashFunctionFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.doubleTypeInfo)) {
            return HiveDoubleBinaryHashFunctionFactory.INSTANCE;
        } else {
            return HiveRawBinaryHashFunctionFactory.INSTANCE;
        }
    }
}
