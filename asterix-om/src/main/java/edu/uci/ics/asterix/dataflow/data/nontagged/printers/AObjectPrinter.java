package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;

public class AObjectPrinter implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final AObjectPrinter INSTANCE = new AObjectPrinter();

    private IPrinter recordPrinter = new ARecordPrinterFactory(null).createPrinter();
    private IPrinter orderedlistPrinter = new AOrderedlistPrinterFactory(null).createPrinter();
    private IPrinter unorderedListPrinter = new AUnorderedlistPrinterFactory(null).createPrinter();

    @Override
    public void init() throws AlgebricksException {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[s]);
        switch (typeTag) {
            case INT8: {
                AInt8Printer.INSTANCE.print(b, s, l, ps);
                break;
            }
            case INT16: {
                AInt16Printer.INSTANCE.print(b, s, l, ps);
                break;
            }
            case INT32: {
                AInt32Printer.INSTANCE.print(b, s, l, ps);
                break;
            }
            case INT64: {
                AInt64Printer.INSTANCE.print(b, s, l, ps);
                break;
            }
            case NULL: {
                ANullPrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case BOOLEAN: {
                ABooleanPrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case FLOAT: {
                AFloatPrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case DOUBLE: {
                ADoublePrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case DATE: {
                ADatePrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case TIME: {
                ATimePrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case DATETIME: {
                ADateTimePrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case DURATION: {
                ADurationPrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case POINT: {
                APointPrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case POINT3D: {
                APoint3DPrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case LINE: {
                ALinePrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case POLYGON: {
                APolygonPrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case CIRCLE: {
                ACirclePrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case STRING: {
                AStringPrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case RECORD: {
                this.recordPrinter.init();
                recordPrinter.print(b, s, l, ps);
                break;
            }
            case ORDEREDLIST: {
                this.orderedlistPrinter.init();
                orderedlistPrinter.print(b, s, l, ps);
                break;
            }
            case UNORDEREDLIST: {
                this.unorderedListPrinter.init();
                unorderedListPrinter.print(b, s, l, ps);
                break;
            }
            default: {
                throw new NotImplementedException("No printer for type " + typeTag);
            }
        }
    }
}