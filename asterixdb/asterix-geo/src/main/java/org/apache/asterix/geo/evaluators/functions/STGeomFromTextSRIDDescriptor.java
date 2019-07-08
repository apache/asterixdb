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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import com.esri.core.geometry.OGCStructure;
import com.esri.core.geometry.OperatorImportFromWkt;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.WktImportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;

public class STGeomFromTextSRIDDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STGeomFromTextSRIDDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_GEOM_FROM_TEXT_SRID;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {

                return new STGeomFromTextSRIDEvaluator(args, ctx);
            }
        };
    }

    private class STGeomFromTextSRIDEvaluator implements IScalarEvaluator {

        private ArrayBackedValueStorage resultStorage;
        private DataOutput out;
        private IPointable inputArg;
        private IScalarEvaluator eval;
        private IPointable inputArg0;
        private IScalarEvaluator eval0;
        private OperatorImportFromWkt wktImporter;

        public STGeomFromTextSRIDEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg = new VoidPointable();
            eval = args[0].createScalarEvaluator(ctx);
            inputArg0 = new VoidPointable();
            eval0 = args[1].createScalarEvaluator(ctx);
            wktImporter = OperatorImportFromWkt.local();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            eval.evaluate(tuple, inputArg);
            byte[] data = inputArg.getByteArray();
            int offset = inputArg.getStartOffset();
            int len = inputArg.getLength();

            eval0.evaluate(tuple, inputArg0);
            byte[] data0 = inputArg0.getByteArray();
            int offset0 = inputArg0.getStartOffset();

            if (data[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data[offset],
                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            }
            if (data0[offset0] != ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data0[offset0],
                        ATypeTag.SERIALIZED_INT64_TYPE_TAG);
            }

            ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset + 1, len - 1);
            DataInputStream dataIn = new DataInputStream(inStream);
            try {
                String geometry = AStringSerializerDeserializer.INSTANCE.deserialize(dataIn).getStringValue();
                int srid = (int) AInt64SerializerDeserializer.getLong(data0, offset0 + 1);
                OGCStructure structure;

                structure = wktImporter.executeOGC(WktImportFlags.wktImportNonTrusted, geometry, null);
                OGCGeometry ogcGeometry = OGCGeometry.createFromOGCStructure(structure, SpatialReference.create(srid));
                ByteBuffer buffer = ogcGeometry.asBinary();
                byte[] wKBGeometryBuffer = buffer.array();
                out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                out.writeInt(wKBGeometryBuffer.length);
                out.write(wKBGeometryBuffer);
                result.set(resultStorage);

            } catch (IOException e) {
                throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e,
                        ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
            }

        }
    }
}
