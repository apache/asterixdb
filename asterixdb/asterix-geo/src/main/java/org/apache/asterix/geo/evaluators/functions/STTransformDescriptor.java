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
import java.io.Serial;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.geo.evaluators.GeoFunctionTypeInferers;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sis.referencing.CRS;
import org.apache.sis.referencing.crs.AbstractCRS;
import org.apache.sis.referencing.cs.AxesConvention;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.FactoryException;

/**
 * ST_Transform(geometry, fromSRID, toSRID) - transforms geometry coordinates
 * from one CRS to another using the 3-argument form where both source and
 * target SRIDs are explicitly specified.
 * CRS WKT definitions are resolved at compile time from metadata and injected
 * via {@link #setImmutableStates(Object...)}.
 */
public class STTransformDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STTransformDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return new GeoFunctionTypeInferers.STTransformTypeInferer();
        }
    };

    @Serial
    private static final long serialVersionUID = 1L;

    private String fromWkt;
    private String toWkt;

    @Override
    public void setImmutableStates(Object... states) {
        this.fromWkt = (String) states[0];
        this.toWkt = (String) states[1];
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_TRANSFORM;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        final String capturedFromWkt = fromWkt;
        final String capturedToWkt = toWkt;
        return new IScalarEvaluatorFactory() {
            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new STTransformEvaluator(args, ctx, capturedFromWkt, capturedToWkt);
            }
        };
    }

    private class STTransformEvaluator implements IScalarEvaluator {
        private final ArrayBackedValueStorage resultStorage;
        private final DataOutput out;
        private final IPointable inputArg0;
        private final IScalarEvaluator eval0;
        private final IPointable inputArg1;
        private final IScalarEvaluator eval1;
        private final IPointable inputArg2;
        private final IScalarEvaluator eval2;
        private final IEvaluatorContext ctx;
        private final MathTransform mathTransform;
        private final TransformFilter transformFilter = new TransformFilter();

        public STTransformEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, String fromWkt, String toWkt)
                throws HyracksDataException {
            resultStorage = new ArrayBackedValueStorage();
            out = resultStorage.getDataOutput();
            inputArg0 = new VoidPointable();
            eval0 = args[0].createScalarEvaluator(ctx);
            inputArg1 = new VoidPointable();
            eval1 = args[1].createScalarEvaluator(ctx);
            inputArg2 = new VoidPointable();
            eval2 = args[2].createScalarEvaluator(ctx);
            this.ctx = ctx;
            // Precompute MathTransform once at evaluator construction time.
            // CRS WKTs were resolved from metadata at compile time on the CC and
            // serialized into this evaluator factory as immutable state.
            try {
                // TODO: expose AxesConvention (e.g. for POSITIVE_RANGE or NORMALIZED)
                //  if a user needs non-default axis handling.
                CoordinateReferenceSystem fromCrs =
                        AbstractCRS.castOrCopy(CRS.fromWKT(fromWkt)).forConvention(AxesConvention.RIGHT_HANDED);
                CoordinateReferenceSystem toCrs =
                        AbstractCRS.castOrCopy(CRS.fromWKT(toWkt)).forConvention(AxesConvention.RIGHT_HANDED);
                this.mathTransform = CRS.findOperation(fromCrs, toCrs, null).getMathTransform();
            } catch (FactoryException e) {
                throw HyracksDataException.create(e);
            }
            transformFilter.setTransform(this.mathTransform);
            LOGGER.debug("[NC runtime] STTransformEvaluator: precomputed MathTransform from WKTs");
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();

            eval0.evaluate(tuple, inputArg0);
            eval1.evaluate(tuple, inputArg1);
            eval2.evaluate(tuple, inputArg2);

            if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1, inputArg2)) {
                return;
            }

            byte[] data0 = inputArg0.getByteArray();
            int offset0 = inputArg0.getStartOffset();
            int len0 = inputArg0.getLength();

            byte[] data1 = inputArg1.getByteArray();
            int offset1 = inputArg1.getStartOffset();

            byte[] data2 = inputArg2.getByteArray();
            int offset2 = inputArg2.getStartOffset();

            ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data0[offset0]);
            if (tag0 != ATypeTag.GEOMETRY) {
                ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(), data0[offset0], 0, ATypeTag.GEOMETRY);
                PointableHelper.setNull(result);
                return;
            }

            try {
                DataInputStream dataIn0 = new DataInputStream(new ByteArrayInputStream(data0, offset0 + 1, len0 - 1));
                Geometry geometry = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn0).getGeometry();

                int fromSRID = readSrid(data1, offset1, 1);
                int toSRID = readSrid(data2, offset2, 2);

                if (geometry.getSRID() != 0 && geometry.getSRID() != fromSRID) {
                    IWarningCollector wc = ctx.getWarningCollector();
                    if (wc.shouldWarn()) {
                        wc.warn(Warning.of(sourceLoc, ErrorCode.SRID_MISMATCH, geometry.getSRID(), fromSRID,
                                getIdentifier().getName()));
                    }
                }

                try {
                    transformGeometry(geometry, transformFilter);
                } catch (HyracksDataException e) {
                    IWarningCollector wc = ctx.getWarningCollector();
                    if (wc.shouldWarn()) {
                        wc.warn(Warning.of(sourceLoc, ErrorCode.CRS_TRANSFORM_FAILED, fromSRID, toSRID,
                                e.getMessage()));
                    }
                    PointableHelper.setNull(result);
                    return;
                }
                geometry.setSRID(toSRID);

                out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                AGeometrySerializerDeserializer.INSTANCE.serialize(geometry, out);
                result.set(resultStorage);
            } catch (IOException e) {
                IWarningCollector wc = ctx.getWarningCollector();
                if (wc.shouldWarn()) {
                    wc.warn(Warning.of(sourceLoc, ErrorCode.INVALID_FORMAT, getIdentifier().getName(),
                            ATypeTag.GEOMETRY));
                }
                PointableHelper.setNull(result);
            }
        }
    }

    private int readSrid(byte[] data, int offset, int argIndex) throws HyracksDataException {
        byte serializedTypeTag = data[offset];
        if (serializedTypeTag == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
            long sridValue = AInt64SerializerDeserializer.getLong(data, offset + 1);
            if (sridValue < Integer.MIN_VALUE || sridValue > Integer.MAX_VALUE) {
                throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                        "SRID value " + sridValue + " is out of range for int");
            }
            return (int) sridValue;
        }
        if (serializedTypeTag == ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
            return AInt32SerializerDeserializer.getInt(data, offset + 1);
        }
        throw new TypeMismatchException(sourceLoc, getIdentifier(), argIndex, serializedTypeTag,
                ATypeTag.SERIALIZED_INT64_TYPE_TAG, ATypeTag.SERIALIZED_INT32_TYPE_TAG);
    }

    static void transformGeometry(Geometry geometry, TransformFilter filter) throws HyracksDataException {
        try {
            geometry.apply(filter);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof TransformException) {
                throw HyracksDataException.create(e.getCause());
            }
            throw HyracksDataException.create(e);
        }
    }

    private static final class TransformFilter implements CoordinateSequenceFilter {
        private final double[] srcPt = new double[2];
        private final double[] dstPt = new double[2];
        private MathTransform mt;

        void setTransform(MathTransform mt) {
            this.mt = mt;
        }

        @Override
        public void filter(CoordinateSequence seq, int i) {
            try {
                srcPt[0] = seq.getX(i);
                srcPt[1] = seq.getY(i);
                mt.transform(srcPt, 0, dstPt, 0, 1);
                seq.setOrdinate(i, CoordinateSequence.X, dstPt[0]);
                seq.setOrdinate(i, CoordinateSequence.Y, dstPt[1]);
            } catch (TransformException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return true;
        }
    }
}
