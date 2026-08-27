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
package org.apache.asterix.lang.common.util;

import static org.apache.asterix.om.vector.VectorIndexParameters.CROSS_POLLINATION_M;
import static org.apache.asterix.om.vector.VectorIndexParameters.DEFAULT_QUANTIZATION;
import static org.apache.asterix.om.vector.VectorIndexParameters.DIMENSION;
import static org.apache.asterix.om.vector.VectorIndexParameters.EPSILON;
import static org.apache.asterix.om.vector.VectorIndexParameters.MAX_CROSS_POLLINATION_M;
import static org.apache.asterix.om.vector.VectorIndexParameters.NUM_CLUSTERS;
import static org.apache.asterix.om.vector.VectorIndexParameters.QUANTIZATION;
import static org.apache.asterix.om.vector.VectorIndexParameters.RNG_FACTOR;
import static org.apache.asterix.om.vector.VectorIndexParameters.SEED;
import static org.apache.asterix.om.vector.VectorIndexParameters.SIMILARITY;
import static org.apache.asterix.om.vector.VectorIndexParameters.TRAIN_LIST_FRACTION;

import java.util.Arrays;
import java.util.Locale;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.vector.VectorQuantization;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.vector.VectorIndexParameters;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Validates the {@code WITH} clause of a {@code CREATE INDEX ... TYPE VTREE} statement and turns it into the
 * typed {@link VectorIndexParameters} carried by {@code CreateIndexStatement}.
 * <p>
 * The parameter names, defaults and bounds all come from {@link VectorIndexParameters}, which also owns how
 * they are persisted; this class only holds the per-parameter value validation and the diagnostics for it.
 */
public class VectorIndexDeclUtil {

    /**
     * Human-readable list of the accepted {@code similarity} values, derived from
     * {@link VectorSimilarityMetric} so it never drifts from the actual set of recognized metrics.
     */
    private static final String ALLOWED_SIMILARITY_VALUES = Arrays.stream(VectorSimilarityMetric.values())
            .map(m -> m.canonical().toUpperCase(Locale.ROOT)).collect(Collectors.joining(", "));

    private VectorIndexDeclUtil() {
    }

    /**
     * Validates the {@code WITH} clause of a vector index and returns its typed, defaulted form.
     * <p>
     * A vector index always has a {@code WITH} clause: the grammar rejects {@code TYPE VTREE} without one, and
     * {@code dimension} and {@code similarity} have no defaults that could stand in. So this never returns
     * {@code null}, and neither does {@code Index.VectorIndexDetails#getVectorParameters()} — consumers read
     * parameters straight off it without a null check.
     *
     * @param withRecord the {@code WITH} record; must not be {@code null}
     */
    public static VectorIndexParameters validateAndGetParameters(RecordConstructor withRecord)
            throws CompilationException {
        return validateAndGetParameters(withRecord, withRecord == null ? null : withRecord.getSourceLocation());
    }

    public static VectorIndexParameters validateAndGetParameters(RecordConstructor withRecord, SourceLocation sourceLoc)
            throws CompilationException {
        if (withRecord == null) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, sourceLoc,
                    "A vector index requires a WITH clause specifying at least `dimension` and `similarity`.");
        }
        AdmObjectNode node = ExpressionUtils.toNode(withRecord);
        validateWithClauseFieldNames(node);

        VectorIndexParameters.Builder builder = VectorIndexParameters.builder();
        builder.setDimension(validateDimension(node));
        builder.setSimilarity(validateSimilarity(node));
        builder.setQuantization(validateQuantization(node));
        builder.setTrainListFraction(validateTrainList(node));
        builder.setEpsilon(validateEpsilon(node));
        validateNumClusters(node).ifPresent(builder::setNumClusters);
        builder.setCrossPollinationM(validateCrossPollinationM(node));
        builder.setRngFactor(validateRngFactor(node));
        builder.setSeed(validateSeed(node));
        try {
            return builder.build();
        } catch (AsterixException e) {
            // Unreachable: the two mandatory parameters are validated above, which reports a clearer error.
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, e, sourceLoc,
                    e.getMessage());
        }
    }

    private static void validateWithClauseFieldNames(AdmObjectNode node) throws CompilationException {
        for (String name : node.getFieldNames()) {
            if (!VectorIndexParameters.isKnown(name)) {
                throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, "Unknown field `"
                        + name + "` in WITH clause. Allowed fields: " + VectorIndexParameters.nameList());
            }
        }
    }

    private static int validateDimension(AdmObjectNode node) throws CompilationException {
        IAdmNode dimNode = node.get(DIMENSION);
        if (dimNode == null) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Missing required parameter `dimension` in WITH clause.");
        }
        if (dimNode.getType() != ATypeTag.BIGINT) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `dimension` parameter value. It must be an integer greater than 0");
        }
        long value = ((AdmBigIntNode) dimNode).get();
        if (value <= 0 || value > Integer.MAX_VALUE) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `dimension` parameter value. It must be an integer greater than 0");
        }
        return (int) value;
    }

    /**
     * Validates {@code similarity} against the single source of truth for metric aliases and returns the
     * resolved metric, so neither the persisted value nor any consumer depends on the alias or casing written.
     */
    private static VectorSimilarityMetric validateSimilarity(AdmObjectNode node) throws CompilationException {
        IAdmNode simNode = node.get(SIMILARITY);
        if (simNode == null) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Missing required parameter `similarity` in WITH clause.");
        }
        if (simNode.getType() != ATypeTag.STRING) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `similarity` parameter value. " + "Allowed values: " + ALLOWED_SIMILARITY_VALUES);
        }
        VectorSimilarityMetric metric = VectorSimilarityMetric.fromAlias(((AdmStringNode) simNode).get());
        if (metric == null) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `similarity` parameter value. " + "Allowed values: " + ALLOWED_SIMILARITY_VALUES);
        }
        return metric;
    }

    /**
     * Validates {@code quantization} and returns the resolved scheme. Optional: an absent value takes
     * {@link VectorIndexParameters#DEFAULT_QUANTIZATION}, since every vector index is quantized.
     */
    private static VectorQuantization validateQuantization(AdmObjectNode node) throws CompilationException {
        IAdmNode qNode = node.get(QUANTIZATION);
        if (qNode == null) {
            return DEFAULT_QUANTIZATION;
        }
        VectorQuantization quantization =
                qNode.getType() == ATypeTag.STRING ? VectorQuantization.fromLabel(((AdmStringNode) qNode).get()) : null;
        if (quantization == null) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `quantization` parameter value. Allowed values: " + VectorQuantization.labelList());
        }
        return quantization;
    }

    /**
     * Training list size is specified only via {@code train_list_fraction} (with ANALYZE/cardinality at build time).
     */
    private static double validateTrainList(AdmObjectNode node) throws CompilationException {
        IAdmNode fn = node.get(TRAIN_LIST_FRACTION);
        if (fn == null) {
            return VectorIndexParameters.DEFAULT_TRAIN_LIST_FRACTION;
        }
        double value = parseDoubleOrBigInt(fn,
                "Invalid `train_list_fraction` parameter value. It must be in the range of (0,1]");
        if (value <= 0 || value > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `train_list_fraction` parameter value. It must be in the range of (0,1]");
        }
        return value;
    }

    private static double validateEpsilon(AdmObjectNode node) throws CompilationException {
        IAdmNode epsNode = node.get(EPSILON);
        if (epsNode == null) {
            return VectorIndexParameters.DEFAULT_EPSILON;
        }
        double value =
                parseDoubleOrBigInt(epsNode, "Invalid `epsilon` parameter value. It must be in the range of [0,1]");
        if (value < 0 || value > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `epsilon` parameter value. It must be in the range of [0,1]");
        }
        return value;
    }

    /**
     * Validates {@code cross_pollination_m}: each record is written into the M closest leaf centroids at
     * bulk-load (M=1 means no cross-pollination, matching legacy behavior). Must be a positive integer;
     * capped at {@link VectorIndexParameters#MAX_CROSS_POLLINATION_M} as a sanity check.
     */
    private static int validateCrossPollinationM(AdmObjectNode node) throws CompilationException {
        IAdmNode mNode = node.get(CROSS_POLLINATION_M);
        if (mNode == null) {
            return VectorIndexParameters.DEFAULT_CROSS_POLLINATION_M;
        }
        if (mNode.getType() != ATypeTag.BIGINT) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `cross_pollination_m` parameter " + "value. It must be an integer in [1, "
                            + MAX_CROSS_POLLINATION_M + "]");
        }
        long value = ((AdmBigIntNode) mNode).get();
        if (value < 1 || value > MAX_CROSS_POLLINATION_M) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `cross_pollination_m` parameter " + "value. It must be an integer in [1, "
                            + MAX_CROSS_POLLINATION_M + "]");
        }
        return (int) value;
    }

    /**
     * Validates {@code num_clusters}: the target number of leaf clusters per storage partition. Optional —
     * when absent the builder derives it as sqrt(cardinality / numPartitions), so no default is injected
     * here. When present it must be a positive BIGINT that fits in an int; a wrong-typed or non-positive
     * value previously slipped past DDL and either threw a ClassCastException during metadata serialization
     * or was silently dropped.
     */
    private static OptionalInt validateNumClusters(AdmObjectNode node) throws CompilationException {
        IAdmNode kNode = node.get(NUM_CLUSTERS);
        if (kNode == null) {
            return OptionalInt.empty();
        }
        if (kNode.getType() != ATypeTag.BIGINT) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `num_clusters` parameter value. " + "It must be an integer greater than 0.");
        }
        long value = ((AdmBigIntNode) kNode).get();
        if (value < 1 || value > Integer.MAX_VALUE) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `num_clusters` parameter value. " + "It must be an integer greater than 0.");
        }
        return OptionalInt.of((int) value);
    }

    /**
     * Validates {@code rng_factor}: SPTAG-style RNG acceptance multiplier applied at bulk-load after the
     * eps-filtered candidate list (see {@code RngAcceptanceFilter}). Must be a positive finite double. A
     * BIGINT literal is coerced to DOUBLE. To effectively disable RNG, use a value much larger than the
     * maximum expected centroid-to-centroid distance.
     */
    private static double validateRngFactor(AdmObjectNode node) throws CompilationException {
        IAdmNode rngNode = node.get(RNG_FACTOR);
        if (rngNode == null) {
            return VectorIndexParameters.DEFAULT_RNG_FACTOR;
        }
        double value = parseDoubleOrBigInt(rngNode,
                "Invalid `rng_factor` parameter " + "value. It must be a positive finite number.");
        if (!(value > 0.0) || !Double.isFinite(value)) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `rng_factor` parameter value. " + "It must be a positive finite number.");
        }
        return value;
    }

    /**
     * Validates {@code seed}, shared by the train-list sample and the k-means RNG. Every {@code long} is a
     * usable seed, so the only check is the type.
     * <p>
     * Unlike the other optional parameters this one has no constant default: when the user does not give a
     * seed we draw one here and persist it, so the index records the seed its build actually used and can be
     * rebuilt identically. Drawing it at DDL time rather than at job-generation time is what makes it
     * persistable — the {@code Metadata.Index} record is written before the creation job is built.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private static long validateSeed(AdmObjectNode node) throws CompilationException {
        IAdmNode seedNode = node.get(SEED);
        if (seedNode == null) {
            return ThreadLocalRandom.current().nextLong();
        }
        if (seedNode.getType() != ATypeTag.BIGINT) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "Invalid `seed` parameter value. It must be an integer.");
        }
        return ((AdmBigIntNode) seedNode).get();
    }

    private static double parseDoubleOrBigInt(IAdmNode n, String errorMsg) throws CompilationException {
        switch (n.getType()) {
            case DOUBLE:
                return ((AdmDoubleNode) n).get();
            case BIGINT:
                return ((AdmBigIntNode) n).get();
            default:
                throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, errorMsg);
        }
    }

}
