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
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.base.IACollection;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;

public class STPolygonizeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STPolygonizeDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_POLYGONIZE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {

                return new STPolygonizeEvaluator(args, ctx);
            }
        };
    }

    private class STPolygonizeEvaluator implements IScalarEvaluator {
        private ArrayBackedValueStorage resultStorage;
        private DataOutput out;
        private IPointable inputArg;
        private IScalarEvaluator eval;

        public STPolygonizeEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg = new VoidPointable();
            eval = args[0].createScalarEvaluator(ctx);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            eval.evaluate(tuple, inputArg);
            byte[] bytes = inputArg.getByteArray();
            int offset = inputArg.getStartOffset();
            int len = inputArg.getLength();

            AOrderedListType type = new AOrderedListType(BuiltinType.AGEOMETRY, null);
            byte typeTag = inputArg.getByteArray()[inputArg.getStartOffset()];
            ISerializerDeserializer serde;
            if (typeTag == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                serde = new AOrderedListSerializerDeserializer(type);
            } else if (typeTag == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                serde = new AOrderedListSerializerDeserializer(type);
            } else {
                throw new TypeMismatchException(sourceLoc, BuiltinFunctions.ST_POLYGONIZE, 0, typeTag,
                        ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG, ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
            }

            ByteArrayInputStream inStream = new ByteArrayInputStream(bytes, offset + 1, len - 1);
            DataInputStream dataIn = new DataInputStream(inStream);
            IACursor cursor = ((IACollection) serde.deserialize(dataIn)).getCursor();
            List<OGCGeometry> list = new ArrayList<>();
            while (cursor.next()) {
                IAObject object = cursor.get();
                list.add(((AGeometry) object).getGeometry());
            }
            OGCGeometryCollection geometryCollection =
                    new OGCConcreteGeometryCollection(list, SpatialReference.create(4326));
            try {
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AGEOMETRY)
                        .serialize(new AGeometry(geometryCollection), out);
            } catch (IOException e) {
                throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e,
                        ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
            }
            result.set(resultStorage);
        }
    }
}
