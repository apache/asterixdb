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
package org.apache.asterix.runtime.unnestingfunctions.std;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;

public class SpatialAttachDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialAttachDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SPATIAL_ATTACH;
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IUnnestingEvaluatorFactory() {

            @Override
            public IUnnestingEvaluator createUnnestingEvaluator(IEvaluatorContext ctx) throws HyracksDataException {

                final IHyracksTaskContext hyracksTaskContext = ctx.getTaskContext();

                return new IUnnestingEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    private final AMutableRectangle aRectangle = new AMutableRectangle(new AMutablePoint(0, 0), new AMutablePoint(0, 0));
                    private final AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };
                    int pos;

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ARectangle> rectangleSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ARECTANGLE);

                    @Override
                    public void init(IFrameTupleReference tuple) throws HyracksDataException {
                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();

                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();

                        ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);
                        ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);

                        if ((tag0 == ATypeTag.RECTANGLE) && (tag1 == ATypeTag.STRING)) {
                            // Get dynamic MBR
                            ByteArrayInputStream keyInputStream =
                                    new ByteArrayInputStream(bytes1, offset1 + 1, inputArg1.getLength() - 1);
                            DataInputStream keyDataInputStream = new DataInputStream(keyInputStream);
                            String key = AStringSerializerDeserializer.INSTANCE.deserialize(keyDataInputStream)
                                    .getStringValue();
                            aRectangle.setValue(aPoint[0], aPoint[1]);
                            pos = 0;
//                            if (TaskUtil.get(key, hyracksTaskContext) != null) {
//                                Double[] mbrCoordinates = TaskUtil.get(key, hyracksTaskContext);
//                                double minX = mbrCoordinates[0];
//                                double minY = mbrCoordinates[1];
//                                double maxX = mbrCoordinates[2];
//                                double maxY = mbrCoordinates[3];
//                                aPoint[0].setValue(minX, minY);
//                                aPoint[1].setValue(maxX, maxY);
//                                aRectangle.setValue(aPoint[0], aPoint[1]);
//                                pos = 0;
//                            } else {
//                                throw HyracksDataException.create(
//                                        new Throwable(String.format("%s: No MBR found", this.getClass().toString())));
//                            }
                        } else {
                            if (tag0 != ATypeTag.RECTANGLE) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                        ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                            }
                            if (tag1 != ATypeTag.STRING) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes1[offset1],
                                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            }
                        }
                    }

                    @Override
                    public boolean step(IPointable result) throws HyracksDataException {
                        if (pos == 0) {
                            resultStorage.reset();
                            rectangleSerde.serialize(aRectangle, resultStorage.getDataOutput());
                            result.set(resultStorage);
                            pos++;
                            return true;
                        }
                        return false;
                    }
                };
            }
        };
    }
}
