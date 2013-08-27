package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class RecordMergeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordMergeDescriptor();
        }
    };

    private ARecordType outRecType;
    private ARecordType inRecType0;
    private ARecordType inRecType1;

    public void reset(ARecordType outRecType, ARecordType inRecType0, ARecordType inRecType1) {
        this.outRecType = outRecType;
        this.inRecType0 = inRecType0;
        this.inRecType1 = inRecType1;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            private final ARecordType recType = RecordMergeDescriptor.this.outRecType;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final PointableAllocator pa = new PointableAllocator();
                final IVisitablePointable vp0 = pa.allocateRecordValue(inRecType0);
                final IVisitablePointable vp1 = pa.allocateRecordValue(inRecType1);

                final ArrayBackedValueStorage abvs0 = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();
                final ICopyEvaluator eval0 = args[0].createEvaluator(abvs0);
                final ICopyEvaluator eval1 = args[1].createEvaluator(abvs1);

                final RecordBuilder rb = new RecordBuilder();
                rb.reset(recType);

                return new ICopyEvaluator() {

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        abvs0.reset();
                        abvs1.reset();
                        rb.init();

                        eval0.evaluate(tuple);
                        eval1.evaluate(tuple);
                        vp0.set(abvs0);
                        vp1.set(abvs1);

                        ARecordPointable rp0 = (ARecordPointable) vp0;
                        ARecordPointable rp1 = (ARecordPointable) vp1;
                        ArrayBackedValueStorage fnvs = new ArrayBackedValueStorage();
                        UTF8StringPointable fnp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
                        try {
                            for (String fieldName : recType.getFieldNames()) {
                                fnvs.reset();
                                UTF8StringSerializerDeserializer.INSTANCE.serialize(fieldName, fnvs.getDataOutput());
                                fnp.set(fnvs);
                                if (!addFieldFromRecord(rp1, fieldName, fnp)) {
                                    addFieldFromRecord(rp0, fieldName, fnp);
                                }
                            }
                            rb.write(output.getDataOutput(), true);
                        } catch (IOException | AsterixException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    private boolean addFieldFromRecord(ARecordPointable rp, String fieldName, UTF8StringPointable fnp)
                            throws IOException, AsterixException {
                        for (int i = 0; i < rp.getFieldNames().size(); ++i) {
                            IVisitablePointable fp = rp.getFieldNames().get(i);
                            IVisitablePointable fv = rp.getFieldValues().get(i);
                            if (fnp.compareTo(fp.getByteArray(), fp.getStartOffset() + 1, fp.getLength() - 1) == 0) {
                                if (recType.isClosedField(fieldName)) {
                                    int pos = recType.findFieldPosition(fieldName);
                                    rb.addField(pos, fv);
                                } else {
                                    rb.addField(fp, fv);
                                }
                                return true;
                            }
                        }
                        return false;
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.RECORD_MERGE;
    }
}
