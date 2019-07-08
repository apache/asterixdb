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
import java.nio.ByteBuffer;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;

public class STMakeEnvelopeDescriptorSRID extends AbstractGetValDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STMakeEnvelopeDescriptorSRID();
        }
    };

    private static final long serialVersionUID = 1L;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_MAKE_ENVELOPE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {

                return new STMakeEnvelopeEvaluator(args, ctx);
            }
        };
    }

    private class STMakeEnvelopeEvaluator implements IScalarEvaluator {

        private ArrayBackedValueStorage resultStorage;
        private DataOutput out;
        private IPointable inputArg0;
        private IScalarEvaluator eval0;
        private IPointable inputArg1;
        private IScalarEvaluator eval1;
        private IPointable inputArg2;
        private IScalarEvaluator eval2;
        private IPointable inputArg3;
        private IScalarEvaluator eval3;
        private IPointable inputArg4;
        private IScalarEvaluator eval4;

        public STMakeEnvelopeEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg0 = new VoidPointable();
            eval0 = args[0].createScalarEvaluator(ctx);
            inputArg1 = new VoidPointable();
            eval1 = args[1].createScalarEvaluator(ctx);
            inputArg2 = new VoidPointable();
            eval2 = args[2].createScalarEvaluator(ctx);
            inputArg3 = new VoidPointable();
            eval3 = args[3].createScalarEvaluator(ctx);
            inputArg4 = new VoidPointable();
            eval4 = args[4].createScalarEvaluator(ctx);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            eval0.evaluate(tuple, inputArg0);
            byte[] data0 = inputArg0.getByteArray();
            int offset0 = inputArg0.getStartOffset();

            eval1.evaluate(tuple, inputArg1);
            byte[] data1 = inputArg1.getByteArray();
            int offset1 = inputArg1.getStartOffset();

            eval2.evaluate(tuple, inputArg2);
            byte[] data2 = inputArg2.getByteArray();
            int offset2 = inputArg2.getStartOffset();

            eval3.evaluate(tuple, inputArg3);
            byte[] data3 = inputArg3.getByteArray();
            int offset3 = inputArg3.getStartOffset();

            eval4.evaluate(tuple, inputArg4);
            byte[] data4 = inputArg4.getByteArray();
            int offset4 = inputArg4.getStartOffset();

            try {

                OGCGeometry ogcGeometry =
                        OGCGeometry
                                .createFromEsriGeometry(
                                        new Envelope(getVal(data0, offset0), getVal(data1, offset1),
                                                getVal(data2, offset2), getVal(data3, offset3)),
                                        SpatialReference.create((int) getVal(data4, offset4)));
                ByteBuffer buffer = ogcGeometry.asBinary();
                byte[] bytes = buffer.array();
                out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                out.writeInt(bytes.length);
                out.write(bytes);
                result.set(resultStorage);
            } catch (IOException e) {
                throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e,
                        ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
            }

        }
    }
}
