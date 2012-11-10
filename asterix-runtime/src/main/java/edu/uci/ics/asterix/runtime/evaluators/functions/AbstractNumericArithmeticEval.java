package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt16;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableInt8;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@SuppressWarnings("serial")
public abstract class AbstractNumericArithmeticEval extends AbstractScalarFunctionDynamicDescriptor {

    
    abstract protected long evaluateInteger(long lhs, long rhs) throws HyracksDataException;
    abstract protected double evaluateDouble(double lhs, double rhs) throws HyracksDataException;
    
    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

                return new ICopyEvaluator() {
                    private DataOutput out = output.getDataOutput();
                    // one temp. buffer re-used by both children
                    private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalLeft = args[0].createEvaluator(argOut);
                    private ICopyEvaluator evalRight = args[1].createEvaluator(argOut);
                    private double[] operandsFloating = new double[args.length];
                    private long[]   operandsInteger  = new long[args.length];
                    private int      resultType;
                    static protected final int typeInt8 = 1;
                    static protected final int typeInt16 = 2;
                    static protected final int typeInt32 = 3;
                    static protected final int typeInt64 = 4;
                    static protected final int typeFloat = 5;
                    static protected final int typeDouble = 6;
                    
                    
                    protected AMutableFloat aFloat = new AMutableFloat(0);
                    protected AMutableDouble aDouble = new AMutableDouble(0);
                    protected AMutableInt64 aInt64 = new AMutableInt64(0);
                    protected AMutableInt32 aInt32 = new AMutableInt32(0);
                    protected AMutableInt16 aInt16 = new AMutableInt16((short) 0);
                    protected AMutableInt8 aInt8 = new AMutableInt8((byte) 0);                    
                    private ATypeTag typeTag;
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            resultType = 0;
                            int currentType = 0;
                            for (int i = 0; i < args.length; i++) {
                                argOut.reset();
                                if (i == 0)
                                    evalLeft.evaluate(tuple);
                                else
                                    evalRight.evaluate(tuple);
                                typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[0]);
                                switch (typeTag) {
                                    case INT8: {   
                                        currentType = typeInt8;
                                        operandsInteger[i] = AInt8SerializerDeserializer.getByte(argOut.getByteArray(), 1);
                                        operandsFloating[i] = AInt8SerializerDeserializer.getByte(argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case INT16: {
                                        currentType = typeInt16;
                                        operandsInteger[i] = AInt16SerializerDeserializer.getShort(argOut.getByteArray(), 1);
                                        operandsFloating[i] = AInt16SerializerDeserializer.getShort(argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case INT32: {
                                        currentType = typeInt32;
                                        operandsInteger[i] = AInt32SerializerDeserializer.getInt(argOut.getByteArray(), 1);
                                        operandsFloating[i] = AInt32SerializerDeserializer.getInt(argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case INT64: {
                                        currentType = typeInt64;
                                        operandsInteger[i] = AInt64SerializerDeserializer.getLong(argOut.getByteArray(), 1);
                                        operandsFloating[i] = AInt64SerializerDeserializer.getLong(argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case FLOAT: {
                                        currentType = typeFloat;
                                        operandsFloating[i] = AFloatSerializerDeserializer.getFloat(argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case DOUBLE: {
                                        currentType = typeDouble;
                                        operandsFloating[i] = ADoubleSerializerDeserializer.getDouble(argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case NULL: {
                                        serde = AqlSerializerDeserializerProvider.INSTANCE
                                                .getSerializerDeserializer(BuiltinType.ANULL);
                                        serde.serialize(ANull.NULL, out);
                                        return;
                                    }
                                    default: {
                                        throw new NotImplementedException(i == 0 ? "Left"
                                                : "Right"
                                                        + " Operand of Division can not be "
                                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut
                                                                .getByteArray()[0]));
                                    }
                                }
                                
                                if(resultType < currentType) {
                                    resultType = currentType;
                                }
                            }
                            
                            long lres = 0;
                            double dres = 0;
                            switch(resultType) {
                                case typeInt8:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AINT8);
                                    lres = evaluateInteger(operandsInteger[0], operandsInteger[1]);
                                    if(lres > Byte.MAX_VALUE) {
                                        throw new AlgebricksException("Overflow happened.");
                                    } 
                                    if(lres < Byte.MIN_VALUE) {
                                        throw new AlgebricksException("Underflow happened.");
                                    }                                     
                                    aInt8.setValue((byte)lres);
                                    serde.serialize(aInt8, out);                
                                    break;                                
                                case typeInt16:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AINT16);
                                    lres = evaluateInteger(operandsInteger[0], operandsInteger[1]);
                                    if(lres > Short.MAX_VALUE) {
                                        throw new AlgebricksException("Overflow happened.");
                                    } 
                                    if(lres < Short.MIN_VALUE) {
                                        throw new AlgebricksException("Underflow happened.");
                                    }                                    
                                    aInt16.setValue((short)lres);
                                    serde.serialize(aInt16, out);                
                                    break;
                                case typeInt32:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AINT32);
                                    lres = evaluateInteger(operandsInteger[0], operandsInteger[1]);
                                    if(lres > Integer.MAX_VALUE) {
                                        throw new AlgebricksException("Overflow happened.");
                                    } 
                                    if(lres < Integer.MIN_VALUE) {
                                        throw new AlgebricksException("Underflow happened.");
                                    }                                      
                                    aInt32.setValue((int)lres);
                                    serde.serialize(aInt32, out);                 
                                    break;
                                case typeInt64:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AINT64);
                                    lres = evaluateInteger(operandsInteger[0], operandsInteger[1]);                                      
                                    aInt64.setValue(lres);
                                    serde.serialize(aInt64, out);                   
                                    break;
                                case typeFloat:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AFLOAT);
                                    dres = evaluateDouble(operandsFloating[0], operandsFloating[1]);
                                    if(dres > Float.MAX_VALUE) {
                                        throw new AlgebricksException("Overflow happened.");
                                    } 
                                    if(dres < - Float.MAX_VALUE) {
                                        throw new AlgebricksException("Underflow happened.");
                                    }                                      
                                    aFloat.setValue((float)dres);
                                    serde.serialize(aFloat, out);                   
                                    break;                                    
                                case typeDouble:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.ADOUBLE);
                                    aDouble.setValue(evaluateDouble(operandsFloating[0], operandsFloating[1]));
                                    serde.serialize(aDouble, out);                   
                                    break;
                            }                                                   
                        } catch (HyracksDataException hde) {
                            throw new AlgebricksException(hde);
                        }
                    }
                };
            }
        };
    }

}
