/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CreateRectangleDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    // allowed input type
    private static final byte SER_POINT_TYPE_TAG = ATypeTag.POINT.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CreateRectangleDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {
                    private DataOutput out = output.getDataOutput();

                    private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval0 = args[0].createEvaluator(outInput0);
                    private ICopyEvaluator eval1 = args[1].createEvaluator(outInput1);
                    private AMutableRectangle aRectangle = new AMutableRectangle(null, null);
                    private AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ARectangle> rectangle2DSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ARECTANGLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        outInput0.reset();
                        eval0.evaluate(tuple);
                        outInput1.reset();
                        eval1.evaluate(tuple);

                        if (outInput0.getByteArray()[0] != SER_POINT_TYPE_TAG
                                || outInput1.getByteArray()[0] != SER_POINT_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.CREATE_RECTANGLE.getName()
                                    + ": expects input type: (POINT, POINT) but got ("
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0])
                                    + ", "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput1.getByteArray()[0])
                                    + ".");
                        }

                        try {
                            aPoint[0].setValue(ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X)),
                                    ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y)));
                            aPoint[1].setValue(ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X)),
                                    ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y)));
                            if (aPoint[0].getX() > aPoint[1].getX() && aPoint[0].getY() > aPoint[1].getY()) {
                                aRectangle.setValue(aPoint[1], aPoint[0]);
                            } else if (aPoint[0].getX() < aPoint[1].getX() && aPoint[0].getY() < aPoint[1].getY()) {
                                aRectangle.setValue(aPoint[0], aPoint[1]);
                            } else {
                                throw new IllegalArgumentException(
                                        "Rectangle arugment must be either (bottom left point, top right point) or (top right point, bottom left point)");
                            }
                            rectangle2DSerde.serialize(aRectangle, out);
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
        return AsterixBuiltinFunctions.CREATE_RECTANGLE;
    }

}