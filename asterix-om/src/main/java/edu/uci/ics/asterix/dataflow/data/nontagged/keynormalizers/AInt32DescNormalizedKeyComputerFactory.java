package edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;

public class AInt32DescNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;

    public static final AInt32DescNormalizedKeyComputerFactory INSTANCE = new AInt32DescNormalizedKeyComputerFactory();

    private IntegerNormalizedKeyComputerFactory inkcf = new IntegerNormalizedKeyComputerFactory();

    private AInt32DescNormalizedKeyComputerFactory() {
    }

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        final INormalizedKeyComputer intNkc = inkcf.createNormalizedKeyComputer();
        return new INormalizedKeyComputer() {

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                int nk = intNkc.normalize(bytes, start + 1, length - 1);
                return (int) ((long) 0xffffffff - (long) nk);
            }
        };
    }
}
