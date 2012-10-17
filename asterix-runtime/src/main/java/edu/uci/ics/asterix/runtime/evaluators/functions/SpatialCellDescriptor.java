package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.Coordinate;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutablePoint;
import edu.uci.ics.asterix.om.base.AMutableRectangle;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.ARectangle;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SpatialCellDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "spatial-cell", 4);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialCellDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private final DataOutput out = output.getDataOutput();

                    private final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage outInput2 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage outInput3 = new ArrayBackedValueStorage();
                    private final ICopyEvaluator eval0 = args[0].createEvaluator(outInput0);
                    private final ICopyEvaluator eval1 = args[1].createEvaluator(outInput1);
                    private final ICopyEvaluator eval2 = args[2].createEvaluator(outInput2);
                    private final ICopyEvaluator eval3 = args[3].createEvaluator(outInput3);
                    private final AMutableRectangle aRectangle = new AMutableRectangle(null, null);
                    private final AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ARectangle> rectangleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ARECTANGLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        outInput0.reset();
                        eval0.evaluate(tuple);
                        outInput1.reset();
                        eval1.evaluate(tuple);
                        outInput2.reset();
                        eval2.evaluate(tuple);
                        outInput3.reset();
                        eval3.evaluate(tuple);

                        try {
                            ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(outInput0.getByteArray()[0]);
                            if (tag == ATypeTag.POINT) {
                                double xLoc = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                double yLoc = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                double xOrigin = ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                double yOrigin = ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                double xInc = ADoubleSerializerDeserializer.getDouble(outInput2.getByteArray(), 1);
                                double yInc = ADoubleSerializerDeserializer.getDouble(outInput3.getByteArray(), 1);

                                double x = xOrigin + (Math.floor((xLoc - xOrigin) / xInc)) * xInc;
                                double y = yOrigin + (Math.floor((yLoc - yOrigin) / yInc)) * yInc;
                                aPoint[0].setValue(x, y);
                                aPoint[1].setValue(x + xInc, y + yInc);
                                aRectangle.setValue(aPoint[0], aPoint[1]);
                                rectangleSerde.serialize(aRectangle, out);
                            } else if (tag == ATypeTag.NULL) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else {
                                throw new NotImplementedException("spatial-cell does not support the type: " + tag
                                        + " It is only implemented for POINT.");
                            }
                        } catch (IOException e1) {
                            throw new AlgebricksException(e1);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}