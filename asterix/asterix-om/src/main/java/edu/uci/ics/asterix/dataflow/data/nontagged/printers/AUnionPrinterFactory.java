package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;
import java.util.List;

import edu.uci.ics.asterix.formats.nontagged.AqlPrinterFactoryProvider;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class AUnionPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    private AUnionType unionType;

    public AUnionPrinterFactory(AUnionType unionType) {
        this.unionType = unionType;
    }

    @Override
    public IPrinter createPrinter() {

        return new IPrinter() {

            private IPrinter[] printers;
            private List<IAType> unionList;

            @Override
            public void init() throws AlgebricksException {
                unionList = unionType.getUnionList();
                printers = new IPrinter[unionType.getUnionList().size()];
                for (int i = 0; i < printers.length; i++) {
                    printers[i] = (AqlPrinterFactoryProvider.INSTANCE
                            .getPrinterFactory(unionType.getUnionList().get(i))).createPrinter();
                    printers[i].init();
                }
            }

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                ATypeTag tag = unionList.get(b[s + 1]).getTypeTag();
                if (tag == ATypeTag.UNION)
                    printers[b[s + 1]].print(b, s + 1, l, ps);
                else {
                    if (tag == ATypeTag.ANY)
                        printers[b[s + 1]].print(b, s + 2, l, ps);
                    else
                        printers[b[s + 1]].print(b, s + 1, l, ps);
                }
            }
        };
    }

}
