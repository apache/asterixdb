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
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
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
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SpatialDistanceDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialDistanceDescriptor();
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
                    private final ICopyEvaluator eval0 = args[0].createEvaluator(outInput0);
                    private final ICopyEvaluator eval1 = args[1].createEvaluator(outInput1);

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        outInput0.reset();
                        eval0.evaluate(tuple);
                        outInput1.reset();
                        eval1.evaluate(tuple);

                        try {
                            byte[] bytes0 = outInput0.getByteArray();
                            byte[] bytes1 = outInput1.getByteArray();
                            ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[0]);
                            ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[0]);
                            double distance = 0.0;
                            if (tag0 == ATypeTag.POINT) {
                                if (tag1 == ATypeTag.POINT) {
                                    double x1 = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                    double y1 = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));
                                    double x2 = ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                    double y2 = ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));
                                    distance = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
                                } else {
                                    throw new NotImplementedException(
                                            AsterixBuiltinFunctions.SPATIAL_DISTANCE.getName()
                                                    + ": does not support the type: " + tag1
                                                    + "; it is only implemented for POINT.");
                                }
                            } else if (tag0 == ATypeTag.NULL || tag1 == ATypeTag.NULL) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else {
                                throw new NotImplementedException(AsterixBuiltinFunctions.SPATIAL_DISTANCE.getName()
                                        + ": does not support the type: " + tag1
                                        + "; it is only implemented for POINT.");
                            }
                            out.writeByte(ATypeTag.DOUBLE.serialize());
                            out.writeDouble(distance);
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
        return AsterixBuiltinFunctions.SPATIAL_DISTANCE;
    }

}