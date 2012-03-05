package edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;

public class AStringAscNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;

    public static final AStringAscNormalizedKeyComputerFactory INSTANCE = new AStringAscNormalizedKeyComputerFactory();

    private AStringAscNormalizedKeyComputerFactory() {
    }

    private UTF8StringNormalizedKeyComputerFactory strnkcf = new UTF8StringNormalizedKeyComputerFactory();

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        final INormalizedKeyComputer strNkc = strnkcf.createNormalizedKeyComputer();
        return new INormalizedKeyComputer() {

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                return strNkc.normalize(bytes, start + 1, length - 1);
            }
        };
    }

}
