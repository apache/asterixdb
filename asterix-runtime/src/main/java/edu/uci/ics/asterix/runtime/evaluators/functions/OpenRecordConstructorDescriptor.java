package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class OpenRecordConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    protected static final FunctionIdentifier FID_OPEN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "open-record-constructor", FunctionIdentifier.VARARGS, true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new OpenRecordConstructorDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private ARecordType recType;
    private boolean[] openFields;

    public void reset(ARecordType recType, boolean[] openFields) {
        this.recType = recType;
        this.openFields = openFields;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID_OPEN;
    }

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                int n = args.length / 2;
                final IEvaluator[] evalNames = new IEvaluator[n];
                final IEvaluator[] evalFields = new IEvaluator[n];
                final ArrayBackedValueStorage fieldNameBuffer = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage fieldValueBuffer = new ArrayBackedValueStorage();
                for (int i = 0; i < n; i++) {
                    evalNames[i] = args[2 * i].createEvaluator(fieldNameBuffer);
                    evalFields[i] = args[2 * i + 1].createEvaluator(fieldValueBuffer);
                }
                final DataOutput out = output.getDataOutput();
                return new IEvaluator() {
                    private RecordBuilder recBuilder = new RecordBuilder();
                    private int closedFieldId;
                    private boolean first = true;

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            closedFieldId = 0;
                            if (first) {
                                first = false;
                                recBuilder.reset(recType);
                            }
                            recBuilder.init();
                            for (int i = 0; i < evalFields.length; i++) {
                                fieldValueBuffer.reset();
                                evalFields[i].evaluate(tuple);
                                if (openFields[i]) {
                                    fieldNameBuffer.reset();
                                    evalNames[i].evaluate(tuple);
                                    recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                                } else {
                                    if (fieldValueBuffer.getBytes()[0] != ATypeTag.NULL.serialize()) {
                                        recBuilder.addField(closedFieldId, fieldValueBuffer);
                                    }
                                    closedFieldId++;
                                }
                            }
                            recBuilder.write(out, true);
                        } catch (IOException ioe) {
                            throw new AlgebricksException(ioe);
                        }
                    }
                };
            }
        };
    }
}
