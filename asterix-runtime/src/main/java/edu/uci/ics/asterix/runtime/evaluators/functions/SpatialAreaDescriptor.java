package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.Coordinate;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.SpatialUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SpatialAreaDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "spatial-area",
            1, true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialAreaDescriptor();
        }
    };

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new IEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    private IEvaluator eval = args[0].createEvaluator(argOut);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        eval.evaluate(tuple);

                        try {
                            byte[] bytes = argOut.getByteArray();
                            ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[0]);
                            double area = 0.0;

                            switch (tag) {
                                case POLYGON:
                                    int numOfPoints = AInt16SerializerDeserializer.getShort(argOut.getByteArray(), 1);

                                    if (numOfPoints < 3) {
                                        throw new AlgebricksException("Polygon must have at least 3 points");
                                    }
                                    area = Math.abs(SpatialUtils.polygonArea(argOut.getByteArray(), numOfPoints));
                                    break;
                                case CIRCLE:
                                    double radius = ADoubleSerializerDeserializer.getDouble(argOut.getByteArray(),
                                            ACircleSerializerDeserializer.getRadiusOffset());
                                    area = SpatialUtils.pi() * radius * radius;
                                    break;
                                case RECTANGLE:
                                    double x1 = ADoubleSerializerDeserializer.getDouble(argOut.getByteArray(),
                                            ARectangleSerializerDeserializer
                                                    .getBottomLeftCoordinateOffset(Coordinate.X));
                                    double y1 = ADoubleSerializerDeserializer.getDouble(argOut.getByteArray(),
                                            ARectangleSerializerDeserializer
                                                    .getBottomLeftCoordinateOffset(Coordinate.Y));

                                    double x2 = ADoubleSerializerDeserializer.getDouble(argOut.getByteArray(),
                                            ARectangleSerializerDeserializer
                                                    .getUpperRightCoordinateOffset(Coordinate.X));
                                    double y2 = ADoubleSerializerDeserializer.getDouble(argOut.getByteArray(),
                                            ARectangleSerializerDeserializer
                                                    .getUpperRightCoordinateOffset(Coordinate.Y));
                                    area = (x2 - x1) * (y2 - y1);
                                    break;
                                default:
                                    throw new NotImplementedException("spatial-area does not support the type: " + tag
                                            + " It is only implemented for POLYGON, CIRCLE and RECTANGLE.");
                            }
                            out.writeByte(ATypeTag.DOUBLE.serialize());
                            out.writeDouble(area);
                        } catch (HyracksDataException hde) {
                            throw new AlgebricksException(hde);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
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