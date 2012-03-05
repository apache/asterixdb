package edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;

public class AStringDescNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;

    public static final AStringDescNormalizedKeyComputerFactory INSTANCE = new AStringDescNormalizedKeyComputerFactory();

    private AStringDescNormalizedKeyComputerFactory() {
    }

    private UTF8StringNormalizedKeyComputerFactory strnkcf = new UTF8StringNormalizedKeyComputerFactory();

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        final INormalizedKeyComputer strNkc = strnkcf.createNormalizedKeyComputer();
        return new INormalizedKeyComputer() {

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                int nk = strNkc.normalize(bytes, start + 1, length - 1);
                return (int) ((long) 0xffffffff - (long) nk);
            }
        };
    }

}
