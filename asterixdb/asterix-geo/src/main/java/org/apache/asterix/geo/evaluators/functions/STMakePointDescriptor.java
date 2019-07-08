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
package org.apache.asterix.geo.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCPoint;

public class STMakePointDescriptor extends AbstractGetValDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STMakePointDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new STMakePointEvaluator(args, ctx);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_MAKE_POINT;
    }

    private class STMakePointEvaluator implements IScalarEvaluator {

        private final ArrayBackedValueStorage resultStorage;
        private final DataOutput out;
        private IPointable inputArg0;
        private IPointable inputArg1;
        private final IScalarEvaluator eval0;
        private final IScalarEvaluator eval1;
        private Point point;
        private AGeometry pointGeometry;

        public STMakePointEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg0 = new VoidPointable();
            inputArg1 = new VoidPointable();
            eval0 = args[0].createScalarEvaluator(ctx);
            eval1 = args[1].createScalarEvaluator(ctx);
            point = new Point(0, 0);
            pointGeometry = new AGeometry(new OGCPoint(point, SpatialReference.create(4326)));
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            eval0.evaluate(tuple, inputArg0);
            eval1.evaluate(tuple, inputArg1);

            byte[] bytes0 = inputArg0.getByteArray();
            int offset0 = inputArg0.getStartOffset();
            byte[] bytes1 = inputArg1.getByteArray();
            int offset1 = inputArg1.getStartOffset();

            resultStorage.reset();
            try {
                out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                point.setX(getVal(bytes0, offset0));
                point.setY(getVal(bytes1, offset1));
                AGeometrySerializerDeserializer.INSTANCE.serialize(pointGeometry, out);
            } catch (IOException e1) {
                throw HyracksDataException.create(e1);
            }
            result.set(resultStorage);
        }
    }
}
