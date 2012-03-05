package edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;

public class AInt32AscNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;

    public static final AInt32AscNormalizedKeyComputerFactory INSTANCE = new AInt32AscNormalizedKeyComputerFactory();

    private IntegerNormalizedKeyComputerFactory inkcf = new IntegerNormalizedKeyComputerFactory();

    private AInt32AscNormalizedKeyComputerFactory() {
    }

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        final INormalizedKeyComputer intNkc = inkcf.createNormalizedKeyComputer();
        return new INormalizedKeyComputer() {

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                return intNkc.normalize(bytes, start + 1, length - 1);
            }
        };
    }

}
