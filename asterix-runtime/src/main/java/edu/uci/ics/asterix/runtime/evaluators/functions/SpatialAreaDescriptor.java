/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.dataflow.data.nontagged.Coordinate;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.SpatialUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SpatialAreaDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialAreaDescriptor();
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
                    private final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    private final ICopyEvaluator eval = args[0].createEvaluator(argOut);

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

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
                                        throw new AlgebricksException(AsterixBuiltinFunctions.SPATIAL_AREA.getName()
                                                + ": polygon must have at least 3 points");
                                    }
                                    area = Math.abs(SpatialUtils.polygonArea(argOut.getByteArray(), numOfPoints));
                                    out.writeByte(ATypeTag.DOUBLE.serialize());
                                    out.writeDouble(area);
                                    break;
                                case CIRCLE:
                                    double radius = ADoubleSerializerDeserializer.getDouble(argOut.getByteArray(),
                                            ACircleSerializerDeserializer.getRadiusOffset());
                                    area = SpatialUtils.pi() * radius * radius;
                                    out.writeByte(ATypeTag.DOUBLE.serialize());
                                    out.writeDouble(area);
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
                                    out.writeByte(ATypeTag.DOUBLE.serialize());
                                    out.writeDouble(area);
                                    break;
                                case NULL:
                                    nullSerde.serialize(ANull.NULL, out);
                                    break;
                                default:
                                    throw new NotImplementedException(AsterixBuiltinFunctions.SPATIAL_AREA.getName()
                                            + ": does not support the type: " + tag
                                            + "; it is only implemented for POLYGON, CIRCLE and RECTANGLE.");
                            }
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
        return AsterixBuiltinFunctions.SPATIAL_AREA;
    }

}