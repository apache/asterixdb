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
package org.apache.asterix.geo.aggregates;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;

/**
 * STUnion aggregates a set of objects into one object. If the input is a set of overlapping polygons, their union is
 * computed and returned as a multipolygon. Similarly, if the input is a set of points or linestring, a multipoint or
 * multilinestring is created. Is the result contains geometries of different types, e.g., points and linestring, the
 * output is a GeometryCollection.
 */
public class STUnionAggregateFunction extends AbstractAggregateFunction {
    /**Use WGS 84 (EPSG:4326) as the default coordinate reference system*/
    public static final SpatialReference DEFAULT_CRS = SpatialReference.create(4326);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AGeometry> geometrySerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AGEOMETRY);
    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;
    protected OGCGeometry geometry;

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    public STUnionAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context, SourceLocation sourceLoc)
            throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
    }

    @Override
    public void init() throws HyracksDataException {
        // Initialize the resulting geometry with an empty point.
        geometry = new OGCPoint(new Point(), DEFAULT_CRS);
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        int len = inputVal.getLength();
        ATypeTag typeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
        // Ignore SYSTEM_NULL.
        if (typeTag == ATypeTag.NULL || typeTag == ATypeTag.MISSING) {
            processNull();
        } else if (typeTag == ATypeTag.GEOMETRY) {
            DataInput dataIn = new DataInputStream(new ByteArrayInputStream(data, offset + 1, len - 1));
            OGCGeometry geometry1 = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn).getGeometry();
            geometry = geometry.union(geometry1);
        }
    }

    @Override
    public void finish(IPointable resultPointable) throws HyracksDataException {
        resultStorage.reset();
        try {
            geometrySerde.serialize(new AGeometry(geometry), resultStorage.getDataOutput());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        resultPointable.set(resultStorage);
    }

    @Override
    public void finishPartial(IPointable resultPointable) throws HyracksDataException {
        finish(resultPointable);
    }

    protected void processNull() throws UnsupportedItemTypeException {
        throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.ST_UNION,
                ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
    }
}
