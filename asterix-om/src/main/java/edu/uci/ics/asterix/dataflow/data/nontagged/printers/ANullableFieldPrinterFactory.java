package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.formats.nontagged.AqlPrinterFactoryProvider;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class ANullableFieldPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    private AUnionType unionType;

    public ANullableFieldPrinterFactory(AUnionType unionType) {
        this.unionType = unionType;
    }

    @Override
    public IPrinter createPrinter() {
        return new IPrinter() {
            private IPrinter nullPrinter;
            private IPrinter fieldPrinter;

            @Override
            public void init() throws AlgebricksException {
                nullPrinter = (AqlPrinterFactoryProvider.INSTANCE.getPrinterFactory(BuiltinType.ANULL)).createPrinter();
                fieldPrinter = (AqlPrinterFactoryProvider.INSTANCE.getPrinterFactory(unionType.getUnionList().get(1)))
                        .createPrinter();
            }

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                fieldPrinter.init();
                if (b[s] == ATypeTag.NULL.serialize())
                    nullPrinter.print(b, s, l, ps);
                else
                    fieldPrinter.print(b, s, l, ps);
            }

        };
    }

}
