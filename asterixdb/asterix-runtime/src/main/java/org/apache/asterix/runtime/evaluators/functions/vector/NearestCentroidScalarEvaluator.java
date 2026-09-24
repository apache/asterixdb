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

package org.apache.asterix.runtime.evaluators.functions.vector;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.asterix.runtime.utils.VectorDistanceFunctionFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * Runtime for {@code nearest_centroid(point, centroids)}: finds the centroid closest to {@code point} under
 * squared-Euclidean distance and returns its 0-based index (AINT32).
 * NULL on missing/invalid input. {@code point} is a list of doubles; {@code centroids} is a list of such lists.
 * Numeric elements are coerced to double; non-numeric elements decode to NaN and yield NULL.
 */
public class NearestCentroidScalarEvaluator implements IScalarEvaluator {

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput dataOutput = resultStorage.getDataOutput();

    private final IScalarEvaluator pointEval;
    private final IScalarEvaluator centroidsEval;
    // Optional third argument naming the metric. The expansion rule emits a literal, so caching by name always hits.
    private final IScalarEvaluator metricEval;
    private final IPointable metricVal = new VoidPointable();
    private final StringBuilder metricName = new StringBuilder();
    private String resolvedMetricName;
    private IVTreeDistanceFunction distanceFn = VectorDistanceCalculation.EUCLIDEAN_SQUARED_FN;
    private final IPointable pointVal = new VoidPointable();
    private final IPointable centroidsVal = new VoidPointable();

    private final ListAccessor pointList = new ListAccessor();
    private final ListAccessor centroidsList = new ListAccessor();
    private final ListAccessor oneCentroidList = new ListAccessor();
    private final IPointable centroidItem = new VoidPointable();
    private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();

    private final VectorListDecoder decoder;
    private double[] pointArr = new double[0];
    private double[] centroidArr = new double[0];

    private final AMutableInt32 aInt32 = new AMutableInt32(0);
    private final ISerializerDeserializer<AInt32> int32Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

    private final IWarningCollector warningCollector;
    private final FunctionIdentifier funcId;
    private final SourceLocation sourceLoc;

    public NearestCentroidScalarEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory[] args,
            FunctionIdentifier funcId, SourceLocation sourceLoc) throws HyracksDataException {
        this.warningCollector = context.getWarningCollector();
        this.funcId = funcId;
        this.sourceLoc = sourceLoc;
        this.decoder = new VectorListDecoder();
        this.pointEval = args[0].createScalarEvaluator(context);
        this.centroidsEval = args[1].createScalarEvaluator(context);
        this.metricEval = args.length > 2 ? args[2].createScalarEvaluator(context) : null;
    }

    /**
     * Resolves the metric argument to a distance function, caching by name. Returns false and warns if the
     * name is not a known metric. DOT uses the same distance as cosine.
     */
    private boolean resolveMetric(IFrameTupleReference tuple) throws HyracksDataException {
        if (metricEval == null) {
            return true;
        }
        metricEval.evaluate(tuple, metricVal);
        byte[] bytes = metricVal.getByteArray();
        int offset = metricVal.getStartOffset();
        if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            warn("nearest_centroid: metric must be a string");
            return false;
        }
        metricName.setLength(0);
        UTF8StringUtil.toString(metricName, bytes, offset + 1);
        String name = metricName.toString();
        if (name.equals(resolvedMetricName)) {
            return true;
        }
        VectorSimilarityMetric metric = VectorSimilarityMetric.fromAlias(name);
        if (metric == null) {
            warn("nearest_centroid: unknown metric '" + name + "'");
            return false;
        }
        distanceFn = new VectorDistanceFunctionFactory(metric).createDistanceFunction();
        resolvedMetricName = name;
        return true;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            pointEval.evaluate(tuple, pointVal);
            centroidsEval.evaluate(tuple, centroidsVal);

            if (!resolveMetric(tuple)) {
                PointableHelper.setNull(result);
                return;
            }

            if (PointableHelper.checkAndSetMissingOrNull(result, pointVal, centroidsVal)) {
                return;
            }
            if (!decoder.checkListType(pointVal) || !decoder.checkListType(centroidsVal)) {
                warn("nearest_centroid expects (list, list-of-lists) arguments");
                PointableHelper.setNull(result);
                return;
            }

            // Decode the point vector (non-numeric elements decode to NaN and poison the distances below).
            pointList.reset(pointVal.getByteArray(), pointVal.getStartOffset());
            int dim = pointList.size();
            if (dim == 0) {
                warn("nearest_centroid: point must be a non-empty numeric vector");
                PointableHelper.setNull(result);
                return;
            }
            pointArr = decoder.createArrayFromList(pointList, decoder.ensureDoubleCapacity(pointArr, dim));

            // Scan the centroids (list of lists) and keep the argmin under the resolved metric.
            centroidsList.reset(centroidsVal.getByteArray(), centroidsVal.getStartOffset());
            int k = centroidsList.size();
            if (k == 0) {
                warn("nearest_centroid: empty centroid set");
                PointableHelper.setNull(result);
                return;
            }

            int bestIdx = -1;
            double bestDist = Double.POSITIVE_INFINITY;
            for (int i = 0; i < k; i++) {
                centroidsList.getOrWriteItem(i, centroidItem, itemStorage);
                if (!decoder.checkListType(centroidItem)) {
                    warn("nearest_centroid: centroid " + i + " is not a vector");
                    PointableHelper.setNull(result);
                    return;
                }
                oneCentroidList.reset(centroidItem.getByteArray(), centroidItem.getStartOffset());
                if (oneCentroidList.size() != dim) {
                    warn("nearest_centroid: centroid " + i + " has wrong dimension");
                    PointableHelper.setNull(result);
                    return;
                }
                centroidArr =
                        decoder.createArrayFromList(oneCentroidList, decoder.ensureDoubleCapacity(centroidArr, dim));
                double d = distanceFn.apply(pointArr, centroidArr);
                if (Double.isNaN(d)) {
                    warn("nearest_centroid: non-numeric point or centroid " + i);
                    PointableHelper.setNull(result);
                    return;
                }
                if (d < bestDist) {
                    bestDist = d;
                    bestIdx = i;
                }
            }

            if (bestIdx < 0) {
                warn("nearest_centroid: no valid centroid found");
                PointableHelper.setNull(result);
                return;
            }
            aInt32.setValue(bestIdx);
            int32Serde.serialize(aInt32, dataOutput);
            result.set(resultStorage);
        } catch (IOException e) {
            warn(e.getMessage());
            PointableHelper.setNull(result);
        }
    }

    private void warn(String msg) {
        if (warningCollector != null && warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(sourceLoc, ErrorCode.FUNCTION_EVALUATION_FAILED, funcId.getName(),
                    msg == null ? "unknown error" : msg));
        }
    }
}
