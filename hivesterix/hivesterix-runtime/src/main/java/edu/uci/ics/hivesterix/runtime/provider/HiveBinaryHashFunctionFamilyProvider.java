package edu.uci.ics.hivesterix.runtime.provider;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;

public class HiveBinaryHashFunctionFamilyProvider implements IBinaryHashFunctionFamilyProvider {

    public static HiveBinaryHashFunctionFamilyProvider INSTANCE = new HiveBinaryHashFunctionFamilyProvider();

    private HiveBinaryHashFunctionFamilyProvider() {

    }

    @Override
    public IBinaryHashFunctionFamily getBinaryHashFunctionFamily(Object type) throws AlgebricksException {
        return MurmurHash3BinaryHashFunctionFamily.INSTANCE;
    }
}
