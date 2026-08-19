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
package org.apache.asterix.om.vector;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The validated configuration of a {@code CREATE INDEX ... TYPE VTREE} index: one typed field per accepted
 * {@code WITH} parameter, and the only place that knows how those parameters are persisted.
 * <p>
 * This is the type carried by {@code CreateIndexStatement} and {@code Index.VectorIndexDetails}. Consumers
 * read parameters through the typed getters rather than by name out of an untyped record, so a misspelled key
 * or a wrong-typed read is a compile error instead of a silent default. An instance is immutable and can only
 * be produced through {@link Builder}, which enforces the two mandatory parameters, so any instance that
 * exists is a complete, usable configuration.
 * <p>
 * Parameters are persisted as <em>open fields of the {@code Metadata.Index} record</em>, one field per
 * parameter, named exactly as written in the {@code WITH} clause. {@link #writeFields} and
 * {@link #readFields} handle each parameter explicitly, one statement each, in {@link #NAMES} order.
 * <p>
 * <b>Adding a parameter therefore means four edits in this file</b>: the field and its getter, a
 * {@link Builder} setter, an entry in {@link #NAMES}, and a line in <em>both</em> {@code writeFields} and
 * {@code readFields}. Miss the write or the read and the parameter is silently dropped when the index is
 * reloaded — the defect this class was extracted to prevent. {@code VectorIndexParametersTupleTranslatorTest}
 * guards the count: add a field without extending {@code NAMES} and it fails.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public final class VectorIndexParameters implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DIMENSION = "dimension";
    public static final String SIMILARITY = "similarity";
    public static final String QUANTIZATION = "quantization";
    public static final String TRAIN_LIST_FRACTION = "train_list_fraction";
    public static final String EPSILON = "epsilon";
    public static final String NUM_CLUSTERS = "num_clusters";
    public static final String CROSS_POLLINATION_M = "cross_pollination_m";
    public static final String RNG_FACTOR = "rng_factor";

    public static final String QUANTIZATION_SQ4 = "SQ4";
    public static final String QUANTIZATION_SQ8 = "SQ8";
    /**
     * Every accepted {@code quantization} label. Both the DDL validator and the build path check against this
     * one declaration, so they cannot drift apart on which labels exist. A {@code List} rather than a
     * {@code Set} because {@link #quantizationList()} renders it into a diagnostic: {@code Set.of} iteration
     * order is randomized per JVM run, which would make that message's wording vary between runs.
     */
    private static final List<String> QUANTIZATION_LABELS = List.of(QUANTIZATION_SQ4, QUANTIZATION_SQ8);
    public static final String DEFAULT_QUANTIZATION = QUANTIZATION_SQ8;
    /** Bit width of {@link #DEFAULT_QUANTIZATION} ({@link #QUANTIZATION_SQ8}); {@link #QUANTIZATION_SQ4} is 4. */
    public static final int DEFAULT_QUANTIZATION_BITS_SQ8 = 8;
    public static final double DEFAULT_TRAIN_LIST_FRACTION = 0.1;
    public static final double DEFAULT_EPSILON = 0.25;
    public static final int DEFAULT_CROSS_POLLINATION_M = 1;
    public static final double DEFAULT_RNG_FACTOR = 1.0;
    /** Sanity cap on {@link #CROSS_POLLINATION_M}: anything larger almost certainly indicates a user error. */
    public static final int MAX_CROSS_POLLINATION_M = 1024;

    /**
     * Every accepted parameter, in persist order. {@code writeFields} emits them in this order, so the bytes
     * written for a given configuration are stable. Kept in step with the two serde methods by hand.
     */
    private static final List<String> NAMES = List.of(DIMENSION, SIMILARITY, QUANTIZATION, TRAIN_LIST_FRACTION, EPSILON,
            NUM_CLUSTERS, CROSS_POLLINATION_M, RNG_FACTOR);

    private final int dimension;
    private final VectorSimilarityMetric similarity;
    private final String quantization;
    private final double trainListFraction;
    private final double epsilon;
    /** {@code null} when unset: the builder then derives it from the dataset cardinality at build time. */
    private final Integer numClusters;
    private final int crossPollinationM;
    private final double rngFactor;

    private VectorIndexParameters(Builder builder) {
        this.dimension = builder.dimension;
        this.similarity = builder.similarity;
        this.quantization = builder.quantization;
        this.trainListFraction = builder.trainListFraction;
        this.epsilon = builder.epsilon;
        this.numClusters = builder.numClusters;
        this.crossPollinationM = builder.crossPollinationM;
        this.rngFactor = builder.rngFactor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getDimension() {
        return dimension;
    }

    /**
     * The distance metric, resolved to its {@link VectorSimilarityMetric} at DDL time and stored canonically,
     * so callers never re-resolve an alias and cannot be handed a spelling the metric taxonomy rejects.
     */
    public VectorSimilarityMetric getSimilarity() {
        return similarity;
    }

    /**
     * The quantization label, never {@code null}: it is optional in the {@code WITH} clause but defaults to
     * {@link #DEFAULT_QUANTIZATION}, and an index record that predates the parameter reads back as that
     * default too. {@code SecondaryVectorOperationsHelper#buildCreationJobSpec} relies on this — it derives
     * the quantization bit width from the label unconditionally, outside any {@link #isQuantized()} guard.
     */
    public String getQuantization() {
        return quantization;
    }

    /**
     * Whether the index stores quantized embeddings, which decides the physical data-tuple layout: the number
     * of secondary key fields in {@code MetadataProvider} and {@code VectorSearchPOperator}, and the tuple
     * builder in {@code VTreeResourceFactoryProvider}. All of those must agree, so they all ask here.
     * <p>
     * Always {@code true} today: every vector index is quantized, because {@code quantization} defaults rather
     * than being absent. The storage and runtime layers still carry a full non-quantized path
     * ({@code VTreeDataTupleAccessor}, {@code LSMVTree#isQuantized}), so this stays the single predicate the
     * layout decisions key off — when a non-quantized mode becomes reachable from DDL, this method is what
     * changes, not its five call sites.
     */
    public boolean isQuantized() {
        return true;
    }

    public double getTrainListFraction() {
        return trainListFraction;
    }

    public double getEpsilon() {
        return epsilon;
    }

    /** Empty when the user did not specify it; the build path then derives it from the dataset cardinality. */
    public OptionalInt getNumClusters() {
        return numClusters == null ? OptionalInt.empty() : OptionalInt.of(numClusters);
    }

    public int getCrossPollinationM() {
        return crossPollinationM;
    }

    public double getRngFactor() {
        return rngFactor;
    }

    /** Every accepted parameter name, in persist order. */
    public static List<String> names() {
        return NAMES;
    }

    /** Only these keys may appear in a vector index {@code WITH} clause. */
    public static boolean isKnown(String name) {
        return NAMES.contains(name);
    }

    /** Comma-separated parameter names, for "allowed fields are ..." diagnostics. */
    public static String nameList() {
        return String.join(", ", NAMES);
    }

    public static boolean isAllowedQuantization(String label) {
        return QUANTIZATION_LABELS.contains(label);
    }

    /** Comma-separated {@code quantization} labels, for "allowed values are ..." diagnostics. */
    public static String quantizationList() {
        return String.join(", ", QUANTIZATION_LABELS);
    }

    /** Bit width the given (already normalized) quantization label encodes at. */
    public static int quantizationBits(String label) {
        return QUANTIZATION_SQ4.equals(label) ? 4 : DEFAULT_QUANTIZATION_BITS_SQ8;
    }

    /**
     * Serializes this configuration into {@code recordBuilder} as one open field per present parameter, in
     * {@link #NAMES} order. Only {@code num_clusters} is genuinely optional, and it is written only when set,
     * so an index that leaves it to the builder still reads back with it unset.
     */
    public void writeFields(IARecordBuilder recordBuilder) throws HyracksDataException {
        FieldWriter writer = new FieldWriter(recordBuilder);
        writer.writeInt(DIMENSION, dimension);
        writer.writeString(SIMILARITY, similarity.canonical());
        writer.writeString(QUANTIZATION, quantization);
        writer.writeDouble(TRAIN_LIST_FRACTION, trainListFraction);
        writer.writeDouble(EPSILON, epsilon);
        if (numClusters != null) {
            writer.writeInt(NUM_CLUSTERS, numClusters);
        }
        writer.writeInt(CROSS_POLLINATION_M, crossPollinationM);
        writer.writeDouble(RNG_FACTOR, rngFactor);
    }

    /**
     * Rebuilds the configuration from the open fields of a persisted {@code Metadata.Index} record. A
     * parameter the record does not carry (an index created before it existed) falls back to its default, so
     * an older index keeps behaving the way it did before the parameter was introduced.
     *
     * @throws AsterixException if the record lost a mandatory parameter, which means corrupt metadata
     */
    public static VectorIndexParameters readFields(ARecord indexRecord) throws AsterixException {
        FieldReader reader = new FieldReader(indexRecord);
        Builder builder = builder();

        Integer dimension = reader.readInt(DIMENSION);
        if (dimension != null) {
            builder.setDimension(dimension);
        }
        String similarity = reader.readString(SIMILARITY);
        if (similarity != null) {
            VectorSimilarityMetric metric = VectorSimilarityMetric.fromAlias(similarity);
            if (metric == null) {
                // Distinguish a corrupt metric from an absent one: leaving it unset would surface as the
                // builder's "missing `similarity`", which sends the reader looking for the wrong problem.
                throw new AsterixException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Unrecognized `" + SIMILARITY + "` value `" + similarity + "` in index metadata");
            }
            builder.setSimilarity(metric);
        }
        // An index record with no `quantization` field predates the parameter; it reads back as the default,
        // which is the label its creation job would have quantized with anyway.
        String quantization = reader.readString(QUANTIZATION);
        if (quantization != null) {
            builder.setQuantization(quantization);
        }
        Double trainListFraction = reader.readDouble(TRAIN_LIST_FRACTION);
        if (trainListFraction != null) {
            builder.setTrainListFraction(trainListFraction);
        }
        Double epsilon = reader.readDouble(EPSILON);
        if (epsilon != null) {
            builder.setEpsilon(epsilon);
        }
        Integer numClusters = reader.readInt(NUM_CLUSTERS);
        if (numClusters != null) {
            builder.setNumClusters(numClusters);
        }
        Integer crossPollinationM = reader.readInt(CROSS_POLLINATION_M);
        if (crossPollinationM != null) {
            builder.setCrossPollinationM(crossPollinationM);
        }
        Double rngFactor = reader.readDouble(RNG_FACTOR);
        if (rngFactor != null) {
            builder.setRngFactor(rngFactor);
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VectorIndexParameters)) {
            return false;
        }
        VectorIndexParameters other = (VectorIndexParameters) o;
        return dimension == other.dimension && Objects.equals(similarity, other.similarity)
                && Objects.equals(quantization, other.quantization)
                && Double.compare(trainListFraction, other.trainListFraction) == 0
                && Double.compare(epsilon, other.epsilon) == 0 && Objects.equals(numClusters, other.numClusters)
                && crossPollinationM == other.crossPollinationM && Double.compare(rngFactor, other.rngFactor) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimension, similarity, quantization, trainListFraction, epsilon, numClusters,
                crossPollinationM, rngFactor);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{ ");
        sb.append(DIMENSION).append(": ").append(dimension);
        sb.append(", ").append(SIMILARITY).append(": ").append(similarity.canonical());
        sb.append(", ").append(QUANTIZATION).append(": ").append(quantization);
        sb.append(", ").append(TRAIN_LIST_FRACTION).append(": ").append(trainListFraction);
        sb.append(", ").append(EPSILON).append(": ").append(epsilon);
        if (numClusters != null) {
            sb.append(", ").append(NUM_CLUSTERS).append(": ").append(numClusters);
        }
        sb.append(", ").append(CROSS_POLLINATION_M).append(": ").append(crossPollinationM);
        sb.append(", ").append(RNG_FACTOR).append(": ").append(rngFactor);
        return sb.append(" }").toString();
    }

    /**
     * Collects parameter values and produces an immutable {@link VectorIndexParameters}. Unset optional
     * parameters take their declared default; {@code dimension} and {@code similarity} have no default and
     * must be set, so a built instance is always a usable configuration.
     */
    public static final class Builder {

        private int dimension = -1;
        private VectorSimilarityMetric similarity;
        private String quantization = DEFAULT_QUANTIZATION;
        private double trainListFraction = DEFAULT_TRAIN_LIST_FRACTION;
        private double epsilon = DEFAULT_EPSILON;
        private Integer numClusters;
        private int crossPollinationM = DEFAULT_CROSS_POLLINATION_M;
        private double rngFactor = DEFAULT_RNG_FACTOR;

        private Builder() {
        }

        public Builder setDimension(int dimension) {
            this.dimension = dimension;
            return this;
        }

        public Builder setSimilarity(VectorSimilarityMetric similarity) {
            this.similarity = similarity;
            return this;
        }

        public Builder setQuantization(String quantization) {
            this.quantization = Objects.requireNonNull(quantization, QUANTIZATION);
            return this;
        }

        public Builder setTrainListFraction(double trainListFraction) {
            this.trainListFraction = trainListFraction;
            return this;
        }

        public Builder setEpsilon(double epsilon) {
            this.epsilon = epsilon;
            return this;
        }

        public Builder setNumClusters(int numClusters) {
            this.numClusters = numClusters;
            return this;
        }

        public Builder setCrossPollinationM(int crossPollinationM) {
            this.crossPollinationM = crossPollinationM;
            return this;
        }

        public Builder setRngFactor(double rngFactor) {
            this.rngFactor = rngFactor;
            return this;
        }

        public boolean hasDimension() {
            return dimension > 0;
        }

        public boolean hasSimilarity() {
            return similarity != null;
        }

        /**
         * @throws AsterixException if a mandatory parameter was never set — at DDL time the validator reports
         *                          the missing key first, so reaching this means corrupt persisted metadata.
         */
        public VectorIndexParameters build() throws AsterixException {
            if (!hasDimension()) {
                throw new AsterixException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Missing `" + DIMENSION + "` parameter in the WITH clause");
            }
            if (!hasSimilarity()) {
                throw new AsterixException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Missing `" + SIMILARITY + "` parameter in the WITH clause");
            }
            return new VectorIndexParameters(this);
        }
    }

    /**
     * Adds one named open field per call, so {@link #writeFields} stays one readable statement per parameter
     * instead of eight copies of the reset / serialize / addField sequence.
     * <p>
     * Each stored type goes through its own {@link ISerializerDeserializer}, so the on-disk tag of a parameter
     * is fixed by which method is called: {@code writeInt} stores {@code INTEGER}, matching what the metadata
     * writer emitted before these parameters moved into this class.
     */
    private static final class FieldWriter {

        private final IARecordBuilder recordBuilder;
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<AInt32> int32Serde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ADouble> doubleSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
        private final ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage();
        private final ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();

        private FieldWriter(IARecordBuilder recordBuilder) {
            this.recordBuilder = recordBuilder;
        }

        private void writeInt(String name, int value) throws HyracksDataException {
            writeName(name);
            fieldValue.reset();
            int32Serde.serialize(new AInt32(value), fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }

        private void writeDouble(String name, double value) throws HyracksDataException {
            writeName(name);
            fieldValue.reset();
            doubleSerde.serialize(new ADouble(value), fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }

        private void writeString(String name, String value) throws HyracksDataException {
            writeName(name);
            fieldValue.reset();
            stringSerde.serialize(new AString(value), fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }

        private void writeName(String name) throws HyracksDataException {
            fieldName.reset();
            stringSerde.serialize(new AString(name), fieldName.getDataOutput());
        }
    }

    /**
     * Reads one named open field per call, returning {@code null} when the record does not carry it so
     * {@link #readFields} can leave the builder's default in place.
     * <p>
     * Narrower numeric tags are accepted so records written before a parameter's storage type was fixed still
     * read back; an outright type mismatch (or a {@code NULL}) reads as absent, so one bad field falls back to
     * its default rather than failing the whole metadata read.
     */
    private static final class FieldReader {

        private final ARecord indexRecord;
        private final ARecordType recordType;

        private FieldReader(ARecord indexRecord) {
            this.indexRecord = indexRecord;
            this.recordType = indexRecord.getType();
        }

        /**
         * @return the value narrowed to {@code int}, or {@code null} when the field is absent, is not numeric,
         *         or holds a value too large to be one — the last of which can only mean corrupt metadata,
         *         since every integer parameter is range-checked at DDL time.
         */
        private Integer readInt(String name) {
            IAObject value = read(name);
            if (value == null) {
                return null;
            }
            Long longValue = asLong(value.getType().getTypeTag(), value);
            if (longValue == null || longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                return null;
            }
            return longValue.intValue();
        }

        private Double readDouble(String name) {
            IAObject value = read(name);
            return value == null ? null : asDouble(value.getType().getTypeTag(), value);
        }

        private String readString(String name) {
            IAObject value = read(name);
            if (value == null || value.getType().getTypeTag() != ATypeTag.STRING) {
                return null;
            }
            return ((AString) value).getStringValue();
        }

        private IAObject read(String name) {
            int position = recordType.getFieldIndex(name);
            return position < 0 ? null : indexRecord.getValueByPos(position);
        }
    }

    private static Long asLong(ATypeTag tag, IAObject value) {
        return switch (tag) {
            case TINYINT -> (long) ((AInt8) value).getByteValue();
            case SMALLINT -> (long) ((AInt16) value).getShortValue();
            case INTEGER -> (long) ((AInt32) value).getIntegerValue();
            case BIGINT -> ((AInt64) value).getLongValue();
            default -> null;
        };
    }

    private static Double asDouble(ATypeTag tag, IAObject value) {
        if (tag == ATypeTag.DOUBLE) {
            return ((ADouble) value).getDoubleValue();
        }
        if (tag == ATypeTag.FLOAT) {
            return (double) ((AFloat) value).getFloatValue();
        }
        Long longValue = asLong(tag, value);
        return longValue == null ? null : longValue.doubleValue();
    }
}
