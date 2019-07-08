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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.geo.evaluators.GeoFunctionTypeInferers;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ARecordType;
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

import com.esri.core.geometry.MapOGCStructure;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.ogc.OGCGeometry;

public class ParseGeoJSONDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ParseGeoJSONDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return new GeoFunctionTypeInferers.GeometryConstructorTypeInferer();
        }
    };

    private static final long serialVersionUID = 1L;
    private ARecordType recType;

    @Override
    public void setImmutableStates(Object... states) {
        this.recType = (ARecordType) states[0];
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.GEOMETRY_CONSTRUCTOR;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {

                return new ParseGeoJSONEvaluator(args[0], ctx);
            }
        };
    }

    private class ParseGeoJSONEvaluator implements IScalarEvaluator {
        private ArrayBackedValueStorage resultStorage;
        private DataOutput out;
        private IPointable inputArg;
        private IScalarEvaluator eval;
        private OperatorImportFromGeoJson geoJsonImporter;

        public ParseGeoJSONEvaluator(IScalarEvaluatorFactory factory, IEvaluatorContext ctx)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg = new VoidPointable();
            eval = factory.createScalarEvaluator(ctx);
            geoJsonImporter = OperatorImportFromGeoJson.local();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            eval.evaluate(tuple, inputArg);
            byte[] data = inputArg.getByteArray();
            int offset = inputArg.getStartOffset();
            int len = inputArg.getLength();

            if (data[offset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                throw new TypeMismatchException(sourceLoc, BuiltinFunctions.GEOMETRY_CONSTRUCTOR, 0, data[offset],
                        ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
            }
            ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset + 1, len - 1);
            DataInput dataIn = new DataInputStream(inStream);
            try {
                String geometry = recordToString(new ARecordSerializerDeserializer(recType).deserialize(dataIn));
                MapOGCStructure structure = geoJsonImporter.executeOGC(0, geometry, null);
                OGCGeometry ogcGeometry =
                        OGCGeometry.createFromOGCStructure(structure.m_ogcStructure, structure.m_spatialReference);
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

        public String recordToString(ARecord record) {
            StringBuilder sb = new StringBuilder();
            sb.append("{ ");
            String[] fieldNames = record.getType().getFieldNames();
            IAObject val;
            if (fieldNames != null) {
                for (int i = 0; i < fieldNames.length; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append("\"").append(fieldNames[i]).append("\"").append(": ");
                    val = record.getValueByPos(i);
                    if (val instanceof ARecord) {
                        sb.append(recordToString((ARecord) val));
                    } else if (val instanceof AOrderedList) {
                        sb.append(listToString((AOrderedList) val));
                    } else {
                        sb.append(val);
                    }
                }
            }
            sb.append(" }");
            return sb.toString();
        }

        public String listToString(AOrderedList list) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            IAObject val;
            sb.append("[ ");
            for (int i = 0; i < list.size(); i++) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                val = list.getItem(i);
                if (val instanceof ARecord) {
                    sb.append(recordToString((ARecord) val));
                } else if (val instanceof AOrderedList) {
                    sb.append(listToString((AOrderedList) val));
                } else {
                    sb.append(val);
                }
            }
            sb.append(" ]");
            return sb.toString();
        }
    }
}
