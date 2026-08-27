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
package org.apache.asterix.metadata.entitytupletranslators;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.vector.VectorQuantization;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.IndexEntity;
import org.apache.asterix.metadata.dataset.DatasetFormatInfo;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.metadata.utils.Creator;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.vector.VectorIndexParameters;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.util.OptionalBoolean;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * Round-trips a vector index's {@link VectorIndexParameters} through {@link IndexTupleTranslator} to guard the
 * failure mode that used to be possible here: a parameter accepted by DDL but not enumerated by the metadata
 * writer was silently dropped on persist.
 * <p>
 * {@code writeFields}/{@code readFields} handle each parameter explicitly, so nothing makes a newly added
 * parameter covered automatically. {@link #declaredFieldsMatchParameterNames} is the tripwire: adding a field
 * without extending {@code NAMES} fails it, and the failure message names the methods to update. Give the new
 * parameter a non-default value in {@link #everyParameterRoundTrips} and the round trip is covered too.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class VectorIndexParametersTupleTranslatorTest {

    /** Every parameter set to a non-default value, so a dropped field cannot look like a correct round trip. */
    @Test
    public void everyParameterRoundTrips() throws AlgebricksException, IOException {
        VectorIndexParameters written =
                VectorIndexParameters.builder().setDimension(128).setSimilarity(VectorSimilarityMetric.EUCLIDEAN)
                        .setQuantization(VectorQuantization.SQ4).setTrainListFraction(0.375).setEpsilon(0.625)
                        .setNumClusters(7).setCrossPollinationM(3).setRngFactor(1.5).setSeed(-9876543210L).build();

        VectorIndexParameters readBack = roundTrip(written);

        Assert.assertEquals(written, readBack);
        Assert.assertEquals(128, readBack.getDimension());
        Assert.assertEquals(VectorSimilarityMetric.EUCLIDEAN, readBack.getSimilarity());
        Assert.assertEquals(VectorQuantization.SQ4, readBack.getQuantization());
        Assert.assertEquals(4, readBack.getQuantization().bits());
        Assert.assertTrue(readBack.isQuantized());
        Assert.assertEquals(0.375, readBack.getTrainListFraction(), 0.0);
        Assert.assertEquals(0.625, readBack.getEpsilon(), 0.0);
        Assert.assertEquals(OptionalInt.of(7), readBack.getNumClusters());
        Assert.assertEquals(3, readBack.getCrossPollinationM());
        Assert.assertEquals(1.5, readBack.getRngFactor(), 0.0);
        // A seed is a full 64-bit value, and a negative one is as valid as any other: storing it as an INTEGER
        // or reading it back through an int would corrupt exactly the seeds the RNG is most likely to draw.
        Assert.assertEquals(-9876543210L, readBack.getSeed());
    }

    /**
     * A parameter the record does not carry — an index created before that parameter existed — reads back as
     * its default rather than as a broken value, and an unset {@code num_clusters} stays unset so the build
     * path still derives it from the dataset cardinality.
     */
    @Test
    public void absentParametersReadBackAsDefaults() throws AlgebricksException, IOException {
        VectorIndexParameters minimal = VectorIndexParameters.builder().setDimension(4)
                .setSimilarity(VectorSimilarityMetric.COSINE).setSeed(0L).build();

        VectorIndexParameters readBack = roundTrip(minimal);

        Assert.assertEquals(minimal, readBack);
        Assert.assertEquals(OptionalInt.empty(), readBack.getNumClusters());
        // 0 is a seed like any other, not an "unset" marker: it must survive the round trip rather than being
        // treated as absent and redrawn.
        Assert.assertEquals(0L, readBack.getSeed());
        Assert.assertEquals(VectorIndexParameters.DEFAULT_TRAIN_LIST_FRACTION, readBack.getTrainListFraction(), 0.0);
        Assert.assertEquals(VectorIndexParameters.DEFAULT_EPSILON, readBack.getEpsilon(), 0.0);
        Assert.assertEquals(VectorIndexParameters.DEFAULT_CROSS_POLLINATION_M, readBack.getCrossPollinationM());
        Assert.assertEquals(VectorIndexParameters.DEFAULT_RNG_FACTOR, readBack.getRngFactor(), 0.0);
        // Quantization is optional in DDL but not nullable: it defaults, so a record that never carried it
        // still reads back with a usable label rather than a null the creation job would have to substitute.
        Assert.assertEquals(VectorIndexParameters.DEFAULT_QUANTIZATION, readBack.getQuantization());
        Assert.assertTrue(readBack.isQuantized());
    }

    /** dimension and similarity have no default, so the builder refuses to produce a partial configuration. */
    @Test
    public void mandatoryParametersAreEnforced() {
        assertBuildRejected(VectorIndexParameters.builder().setSimilarity(VectorSimilarityMetric.EUCLIDEAN),
                VectorIndexParameters.DIMENSION);
        assertBuildRejected(VectorIndexParameters.builder().setDimension(4), VectorIndexParameters.SIMILARITY);
    }

    /**
     * seed is the one parameter with neither a constant default nor a mandatory-set check: an unset one is
     * drawn, so a persisted record that lost the field still loads. Two seedless builders must not agree, or
     * the "draw" is really a fixed default and every such index would share one seed.
     */
    @Test
    public void unsetSeedIsDrawnRatherThanRejected() throws AlgebricksException {
        // Independent draws collide with probability 2^-64.
        Assert.assertNotEquals(seedlessBuilder().build().getSeed(), seedlessBuilder().build().getSeed());
    }

    /** Rebuilding one builder repeats its drawn seed, so two reads of the same record cannot disagree. */
    @Test
    public void aDrawnSeedIsStableAcrossRebuilds() throws AlgebricksException {
        VectorIndexParameters.Builder builder = seedlessBuilder();

        Assert.assertEquals(builder.build().getSeed(), builder.build().getSeed());
        Assert.assertEquals(builder.build(), builder.build());
    }

    private static VectorIndexParameters.Builder seedlessBuilder() {
        return VectorIndexParameters.builder().setDimension(4).setSimilarity(VectorSimilarityMetric.COSINE);
    }

    /**
     * Every instance field must have a matching entry in {@code NAMES}. This cannot prove {@code writeFields}
     * and {@code readFields} were updated, but it fails the moment a parameter is added, which is the point at
     * which both need a line — and {@link #everyParameterRoundTrips} then catches a missing one.
     */
    @Test
    public void declaredFieldsMatchParameterNames() {
        List<String> fields = new ArrayList<>();
        for (Field field : VectorIndexParameters.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic()) {
                fields.add(field.getName());
            }
        }
        Assert.assertEquals(
                "VectorIndexParameters declares " + fields + " but NAMES has " + VectorIndexParameters.names()
                        + "; a new parameter needs an entry in NAMES and a line in both writeFields and readFields",
                VectorIndexParameters.names().size(), fields.size());
    }

    /** A vector index always carries a validated configuration; consumers rely on it being non-null. */
    @Test(expected = NullPointerException.class)
    public void parametersAreRequired() {
        vectorIndexDetails(null);
    }

    private static void assertBuildRejected(VectorIndexParameters.Builder builder, String missingParameter) {
        try {
            builder.build();
            Assert.fail("expected a missing `" + missingParameter + "` to be rejected");
        } catch (AlgebricksException e) {
            Assert.assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains(missingParameter));
        }
    }

    private static Index.VectorIndexDetails vectorIndexDetails(VectorIndexParameters parameters) {
        return new Index.VectorIndexDetails(Collections.singletonList("embedding"), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(), false, OptionalBoolean.empty(), parameters);
    }

    private static VectorIndexParameters roundTrip(VectorIndexParameters parameters)
            throws AlgebricksException, IOException {
        Map<String, String> compactionPolicyProperties = new HashMap<>();
        compactionPolicyProperties.put("max-mergable-component-size", "1073741824");
        compactionPolicyProperties.put("max-tolerance-component-count", "3");

        List<List<String>> primaryKey = Collections.singletonList(Collections.singletonList("row_id"));
        InternalDatasetDetails details =
                new InternalDatasetDetails(FileStructure.BTREE, PartitioningStrategy.HASH, primaryKey, primaryKey,
                        Collections.singletonList(0), Collections.singletonList(BuiltinType.AINT64), false, null, null);

        DataverseName dvTest = DataverseName.createSinglePartName("test");
        DataverseName dvFoo = DataverseName.createSinglePartName("foo");
        DataverseName dvCB = DataverseName.createSinglePartName("CB");
        String dvTestDatabase = MetadataUtil.databaseFor(dvTest);
        Dataset dataset = new Dataset(dvTestDatabase, dvTest, "d1", MetadataUtil.databaseFor(dvFoo), dvFoo, "LogType",
                MetadataUtil.databaseFor(dvCB), dvCB, "MetaType", "DEFAULT_NG_ALL_NODES", "prefix",
                compactionPolicyProperties, details, Collections.emptyMap(), DatasetType.INTERNAL, 115, 0,
                CompressionManager.NONE, DatasetFormatInfo.SYSTEM_DEFAULT, Creator.DEFAULT_CREATOR);

        Index index = new Index(dvTestDatabase, dvTest, "d1", "idx_emb", IndexType.VTREE,
                vectorIndexDetails(parameters), false, false, MetadataUtil.PENDING_NO_OP, Creator.DEFAULT_CREATOR);

        MetadataNode mockMetadataNode = mock(MetadataNode.class);
        when(mockMetadataNode.getDatatype(any(), anyString(), any(DataverseName.class), anyString())).thenReturn(
                new Datatype(dvTestDatabase, dvTest, "d1", new ARecordType("", new String[] { "row_id", "embedding" },
                        new IAType[] { BuiltinType.AINT64, BuiltinType.ANY }, true), true));
        when(mockMetadataNode.getDataset(any(), anyString(), any(DataverseName.class), anyString()))
                .thenReturn(dataset);

        IndexTupleTranslator translator = new IndexTupleTranslator(null, mockMetadataNode, true, IndexEntity.of(false));
        ITupleReference tuple = translator.getTupleFromMetadataEntity(index);
        Index deserialized = translator.getMetadataEntityFromTuple(tuple);
        return ((Index.VectorIndexDetails) deserialized.getIndexDetails()).getVectorParameters();
    }
}
