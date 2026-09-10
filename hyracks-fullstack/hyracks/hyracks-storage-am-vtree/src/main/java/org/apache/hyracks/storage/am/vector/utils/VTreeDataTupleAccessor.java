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

package org.apache.hyracks.storage.am.vector.utils;

import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Single authority for the VTree DATA-tuple field layout
 * {@code [distance, centroidId, (quantized_distance, quantized_embedding,) key..., value...]}.
 * <p>
 * Owns the field indices (as static constants), the {@code isQuantized}-dependent derivations, and the
 * typed field reads so that no other class hardcodes a field position. The layout is a function of
 * {@code isQuantized} alone — the caller-supplied key fields, and the value fields after them, simply
 * follow this access method's own leading fields — so the accessor is immutable and
 * trivially constructible anywhere that flag is known. The writer {@code VTreeDataTupleBuilder} builds
 * tuples in this exact order, so the two must stay in lock-step.
 * <p>
 * "Key fields" are the caller's fields that complete the ordering key after the distance; "value
 * fields" are the payload it asked to carry. This class names them structurally and does not know what
 * a caller puts there, which is why {@code comparatorFields} rather than a field count is what the
 * frames are given.
 * <p>
 * Two formats are supported, determined by the presence of a "description" field in the DDL WITH clause
 * at index creation time:
 * <pre>
 *   Non-quantized (description == null):
 *     | distance(0) | centroidId(1) | key...(2+) | value...(N+) |
 *   Quantized (description != null):
 *     | distance(0) | centroidId(1) | quantized_distance(2) | quantized_embedding(3) | key...(4+) | value...(N+) |
 * </pre>
 */
public final class VTreeDataTupleAccessor {

    // ---------- page-chain sentinels ----------

    /** Value stored in a frame's next-page pointer to mark the end of a page chain. */
    public static final int NO_NEXT_PAGE = -1;

    /** Value stored in a per-cluster directory-page slot before a real page id is assigned. */
    public static final int UNASSIGNED_DIR_PAGE = -1;

    // ---------- non-quantized format field offsets (description == null) ----------

    /** Distance field index (same in both formats). */
    public static final int DISTANCE_FIELD = 0;

    /** Centroid ID field index in non-quantized format. */
    public static final int NQ_CENTROID_ID_FIELD = 1;

    /** First caller-supplied key field index in non-quantized format. */
    public static final int NQ_KEY_FIELDS_START = 2;

    /** Number of this access method's own leading fields in non-quantized format. */
    public static final int NQ_NUM_SECONDARY_FIELDS = 2;

    // ---------- quantized format field offsets (description != null) ----------
    // Layout: [distance(0), centroidId(1), quantized_distance(2), quantized_embedding(3), key...(4+)]
    // This matches createTransformedTuple() in VTreeBulkLoaderAndGroupingOperatorDescriptor.

    /** Centroid ID field index in quantized format. */
    public static final int Q_CENTROID_ID_FIELD = 1;

    /** Quantized distance field index in quantized format. */
    public static final int Q_QUANTIZED_DISTANCE_FIELD = 2;

    /** Quantized embedding field index in quantized format. */
    public static final int Q_QUANTIZED_EMBEDDING_FIELD = 3;

    /** First caller-supplied key field index in quantized format. */
    public static final int Q_KEY_FIELDS_START = 4;

    /** Number of this access method's own leading fields in quantized format. */
    public static final int Q_NUM_SECONDARY_FIELDS = 4;

    /** Positions of the {@code count} identity fields that follow this access method's own fields. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    public static int[] identityFields(boolean isQuantized, int count) {
        int start = getNumSecondaryFields(isQuantized);
        int[] fields = new int[count];
        for (int i = 0; i < count; i++) {
            fields[i] = start + i;
        }
        return fields;
    }

    /** Returns the number of leading own fields (before the key and value fields) for this mode. */
    public static int getNumSecondaryFields(boolean isQuantized) {
        return isQuantized ? Q_NUM_SECONDARY_FIELDS : NQ_NUM_SECONDARY_FIELDS;
    }

    /** Returns the centroid ID field index for the given quantization mode. */
    public static int getCentroidIdField(boolean isQuantized) {
        return isQuantized ? Q_CENTROID_ID_FIELD : NQ_CENTROID_ID_FIELD;
    }

    private final boolean quantized;

    public VTreeDataTupleAccessor(boolean quantized) {
        this.quantized = quantized;
    }

    public boolean isQuantized() {
        return quantized;
    }

    // ---------- field indices (the single place these are derived) ----------

    public int distanceField() {
        return DISTANCE_FIELD;
    }

    public int centroidIdField() {
        return getCentroidIdField(quantized);
    }

    /** Quantized-distance field index, or {@code -1} for a non-quantized layout. */
    public int quantizedDistanceField() {
        return quantized ? Q_QUANTIZED_DISTANCE_FIELD : -1;
    }

    /** Quantized-embedding field index, or {@code -1} for a non-quantized layout. */
    public int quantizedEmbeddingField() {
        return quantized ? Q_QUANTIZED_EMBEDDING_FIELD : -1;
    }

    public int numSecondaryFields() {
        return getNumSecondaryFields(quantized);
    }

    // ---------- typed reads ----------

    /** Distance-to-centroid in field 0 (raw big-endian double, no type tag). */
    public double getDistance(ITupleReference tuple) {
        int f = distanceField();
        return DoublePointable.getDouble(tuple.getFieldData(f), tuple.getFieldStart(f));
    }

    /** Centroid id in field 1 (raw big-endian int, no type tag). */
    public int getCentroidId(ITupleReference tuple) {
        int f = centroidIdField();
        return IntegerPointable.getInteger(tuple.getFieldData(f), tuple.getFieldStart(f));
    }

    /**
     * Content bytes of the quantized embedding (field 3) with the {@link ByteArrayPointable} varlen
     * length prefix stripped. Only valid on a quantized layout.
     */
    public byte[] getQuantizedEmbedding(ITupleReference tuple) {
        int f = quantizedEmbeddingField();
        byte[] data = tuple.getFieldData(f);
        int offset = tuple.getFieldStart(f);
        int contentLength = ByteArrayPointable.getContentLength(data, offset);
        int prefixSize = ByteArrayPointable.getNumberBytesToStoreMeta(contentLength);
        byte[] content = new byte[contentLength];
        System.arraycopy(data, offset + prefixSize, content, 0, contentLength);
        return content;
    }
}
