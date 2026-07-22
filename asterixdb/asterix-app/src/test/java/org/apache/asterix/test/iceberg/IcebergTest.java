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
package org.apache.asterix.test.iceberg;

import static org.apache.asterix.api.common.LocalCloudUtilAdobeMock.fillConfigTemplate;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.CONFIG_FILE;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.CONFIG_FILE_TEMPLATE;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.MOCK_SERVER_HOSTNAME_FRAGMENT;
import static org.apache.hyracks.util.file.FileUtil.joinPath;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.api.common.LocalCloudUtilAdobeMock;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.external.awsclient.EnsureCloseAWSClientFactory;
import org.apache.asterix.external.util.aws.AwsConstants;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.asterix.external.util.iceberg.nessie.NessieUtils;
import org.apache.asterix.test.common.TestConstants;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.VariantShreddingFunction;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * Runs an AWS S3 mock server and test for iceberg catalogs and tables
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IcebergTest {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String SUITE_TESTS = "testsuite_iceberg.xml";
    private static final String ONLY_TESTS = "testsuite_iceberg_only.xml";

    // s3 config
    private static final String MOCK_SERVER_REGION = "us-west-2";
    private static final String ICEBERG_CONTAINER = "iceberg-container";
    private static final String ALL_ICEBERG_TYPES_PATH = joinPath("data", "json", "iceberg_all_types.json");
    private static S3MockContainer s3Mock;

    // Nessie config
    private static GenericContainer<?> nessie;
    private static final DockerImageName NESSIE_IMAGE = DockerImageName.parse("ghcr.io/projectnessie/nessie:0.107.5");
    private static final int NESSIE_PORT = 19120;
    private static final String NESSIE_URI = "http://localhost:" + NESSIE_PORT + "/api/v2";
    private static final String NESSIE_WAREHOUSE = "s3://" + ICEBERG_CONTAINER + "/nessie/warehouse";
    private static final Namespace NAMESPACE = Namespace.of("my_namespace");
    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "users");
    private static final TableIdentifier ALL_TYPES_TABLE_ID = TableIdentifier.of(NAMESPACE, "allTypes");
    private static final TableIdentifier ALL_TYPES_VARIANT_TABLE_ID = TableIdentifier.of(NAMESPACE, "allTypesVariant");
    private static final TableIdentifier DEPTH_TEST_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "depthTestVariant");
    private static final TableIdentifier SHREDDED_VARIANT_TABLE_ID = TableIdentifier.of(NAMESPACE, "shreddedVariant");
    private static final TableIdentifier MANY_FILES_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "manyFilesVariant");
    private static final TableIdentifier DOTTED_FIELD_NAME_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "dottedFieldNameVariant");
    private static final TableIdentifier MIXED_SHREDDING_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "mixedShreddingVariant");
    /** Files whose variant is fully shredded; the rest shred only "name", so their "bucket" has no bounds. */
    private static final int MIXED_SHREDDING_SHREDDED_FILES = 3;
    private static final int MIXED_SHREDDING_FILE_COUNT = 6;
    // One shredded single-row data file per bucket value (0..N-1). The variant-stats-pushdown query suite only
    // Do not change this without re-checking the suites that depend on it. variant-predicate-shapes filters on
    // "bucket < 98" and expects the last two buckets, so it needs at least 100; the flag-off step of
    // variant-stats-pushdown asserts processedObjects equals the whole file count, so it needs exactly this number.
    private static final int MANY_FILES_VARIANT_FILE_COUNT = 100;
    private static final TableIdentifier DELETES_VARIANT_TABLE_ID = TableIdentifier.of(NAMESPACE, "deletesVariant");
    // Same per-bucket shredded layout as manyFilesVariant, but with equality deletes. The variant-deletes query suite
    // only references buckets 0..5, so any count >= 6 keeps its expected results valid; kept above that so a pruned
    // scan is a visible fraction of the table.
    private static final int DELETES_VARIANT_FILE_COUNT = 20;
    /** Buckets whose (single) row is removed by an equality delete. Row id is bucket + 1. */
    private static final int[] DELETES_VARIANT_DELETED_BUCKETS = { 2, 3 };
    private static final TableIdentifier DELETION_VECTOR_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "deletionVectorVariant");
    // Single-row files for buckets 0..N-1, plus one extra file holding DELETION_VECTOR_MULTI_ROW_BUCKETS. The
    // variant-deletion-vectors suite references buckets 0..5 and the 100..102 group only, so any count >= 6 keeps its
    // expected results valid.
    private static final int DELETION_VECTOR_VARIANT_FILE_COUNT = 20;
    /** The single-row file emptied by a deletion vector. Row id is bucket + 1. */
    private static final int DELETION_VECTOR_DELETED_BUCKET = 3;
    /**
     * Buckets sharing one data file, so a deletion vector can remove only the middle row (bucket 101, at position 1)
     * and leave the other two. Far from the single-row range so a predicate selects one group or the other.
     */
    private static final int[] DELETION_VECTOR_MULTI_ROW_BUCKETS = { 100, 101, 102 };
    /**
     * A second multi-row file whose deletion vector removes MORE THAN ONE position. Every other DV in this fixture
     * deletes a single position, so without this the multi-position encoding of the roaring bitmap is never exercised.
     * Buckets sit in a range no other step in the suite selects, so this file changes no existing expectation.
     */
    private static final int[] DELETION_VECTOR_MULTI_DELETE_BUCKETS = { 50, 51, 52, 53 };
    private static final TableIdentifier MANY_ROW_GROUPS_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "manyRowGroupsVariant");
    /**
     * One data file holding every row, deliberately written with a tiny Parquet row-group size so it contains many
     * row groups. Every other fixture file has a single row group, so this is the only thing that makes the reader's
     * row-group loop iterate. The row count and row-group size are paired: the fixture asserts more than one row
     * group was actually produced, so the test cannot silently degrade into the single-row-group case.
     */
    private static final int MANY_ROW_GROUPS_VARIANT_ROW_COUNT = 5000;
    private static final int MANY_ROW_GROUPS_ROW_GROUP_BYTES = 16384;
    private static final TableIdentifier NESTED_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "nestedVariantInStruct");
    private static final int NESTED_VARIANT_ROW_COUNT = 4;
    private static final TableIdentifier PARTITIONED_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "partitionedVariant");
    /** Buckets 0..N-1, one row per file, grouped into partitions of PARTITIONED_VARIANT_GROUP_SIZE consecutive buckets. */
    private static final int PARTITIONED_VARIANT_FILE_COUNT = 12;
    private static final int PARTITIONED_VARIANT_GROUP_SIZE = 3;
    private static final TableIdentifier TWO_VARIANTS_TABLE_ID = TableIdentifier.of(NAMESPACE, "twoVariants");
    private static final int TWO_VARIANTS_FILE_COUNT = 6;
    private static final TableIdentifier PARTIALLY_SHREDDED_VARIANT_TABLE_ID =
            TableIdentifier.of(NAMESPACE, "partiallyShreddedVariant");

    protected TestCaseContext tcCtx;

    public IcebergTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        assertFlagsOffExpectationsAreCopies();
        final TestExecutor testExecutor = new TestExecutor();
        LOGGER.info("Starting S3 mock and Nessie containers");
        s3Mock = LocalCloudUtilAdobeMock.startS3CloudEnvironment(true);
        nessie = new GenericContainer<>(NESSIE_IMAGE).withExposedPorts(NESSIE_PORT);
        nessie.setPortBindings(List.of(NESSIE_PORT + ":" + NESSIE_PORT));
        nessie.start();
        testExecutor.setNessieEndpointDefault(NESSIE_URI);

        prepareIcebergContainer();
        prepareIcebergData();

        testExecutor.executorId = "cloud";
        testExecutor.stripSubstring = "//DB:";
        fillConfigTemplate(MOCK_SERVER_HOSTNAME_FRAGMENT + s3Mock.getHttpServerPort(), CONFIG_FILE_TEMPLATE,
                CONFIG_FILE);
        System.setProperty(TestConstants.S3_SERVICE_ENDPOINT_KEY,
                MOCK_SERVER_HOSTNAME_FRAGMENT + s3Mock.getHttpServerPort());
        LangExecutionUtil.setUp(CONFIG_FILE, testExecutor);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, CONFIG_FILE);
    }

    /**
     * The variant-flags-off suite runs variant-shredded's queries with both pushdown flags disabled, and shows the
     * optimized and unoptimized reads agree by reusing variant-shredded's expected files verbatim. Nothing in the test
     * framework ties the two copies together, so regenerating them independently would leave a suite that still passes
     * but no longer compares the two reads - only each read against itself. Checked here rather than as a test case
     * because it is a property of the fixtures, and failing before the containers start makes the cause obvious.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    private static void assertFlagsOffExpectationsAreCopies() throws IOException {
        Path results = Paths.get("src", "test", "resources", "runtimets", "results", "iceberg");
        for (String fileName : List.of("result.020.adm", "result.030.adm")) {
            Path optimized = results.resolve("variant-shredded").resolve(fileName);
            Path flagsOff = results.resolve("variant-flags-off").resolve(fileName);
            Assert.assertArrayEquals(
                    fileName + ": variant-flags-off's expected file must stay byte-identical to "
                            + "variant-shredded's - the suite proves the two reads agree only for as long as it reuses the "
                            + "same expectation. If variant-shredded's output changed legitimately, copy it across rather "
                            + "than regenerating the two independently.",
                    Files.readAllBytes(optimized), Files.readAllBytes(flagsOff));
        }
    }

    @Parameters(name = "IcebergTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }

    private static void prepareIcebergContainer() {
        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_HOSTNAME_FRAGMENT + s3Mock.getHttpServerPort()); // endpoint pointing to S3 mock server
        LOGGER.info("Creating bucket {} via mock endpoint {}", ICEBERG_CONTAINER, endpoint);
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        S3Client client = builder.build();
        client.createBucket(CreateBucketRequest.builder().bucket(ICEBERG_CONTAINER).build());
        LOGGER.info("Created bucket {}", ICEBERG_CONTAINER);
        client.close();
    }

    private static void prepareIcebergData() throws Exception {
        LOGGER.info("[START] IcebergTest running…");
        try (org.apache.iceberg.nessie.NessieCatalog catalog = createNessieCatalog()) {
            ensureNamespace(catalog, NAMESPACE);
            writeUserTable(catalog);
            writeAllTypesTable(catalog);
            writeAllTypesVariantTable(catalog);
            writeDepthTestVariantTable(catalog);
            writeShreddedVariantTable(catalog);
            writePartiallyShreddedVariantTable(catalog);
            writeManyFilesVariantTable(catalog);
            writeDeletesVariantTable(catalog);
            writeDeletionVectorVariantTable(catalog);
            writeManyRowGroupsVariantTable(catalog);
            writeNestedVariantInStructTable(catalog);
            writePartitionedVariantTable(catalog);
            writeTwoVariantsTable(catalog);
            writeDottedFieldNameVariantTable(catalog);
            writeMixedShreddingVariantTable(catalog);
        }
    }

    private static void writeUserTable(org.apache.iceberg.nessie.NessieCatalog catalog) throws Exception {
        Schema schemaV1 = buildSchemaV1();
        PartitionSpec spec = buildPartitionSpec(schemaV1);
        Table table = createTable(catalog, TABLE_ID, schemaV1, spec);

        LOGGER.info("[TABLE] name={}", table.name());
        LOGGER.info("[TABLE] location={}", table.location());
        LOGGER.info("[TABLE] spec={}", table.spec());
        LOGGER.info("[TABLE] schema(v1)={}", table.schema());
        GenericRecord templateV1 = GenericRecord.create(table.schema());

        long snap1 = writeSnapshot(table, templateV1, 1, 800);
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        TestConstants.Iceberg.snapshot1SnapshotIdValue = snap1;
        TestConstants.Iceberg.snapshot1TimestampLongValue = System.currentTimeMillis();
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        evolveSchemaAddColumn(table);
        LOGGER.info("[SCHEMA EVOLUTION] schema(now)={}", table.schema());
        GenericRecord templateV2 = GenericRecord.create(table.schema());
        long snap2 = writeSnapshot(table, templateV2, 801, 1000);
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        TestConstants.Iceberg.snapshot2TimestampDatetimeValue =
                LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).toString();
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        LOGGER.info("[DELETE] Committing equality deletes for ids=[10, 20, 30]");
        int[] idsToDelete = { 10, 20, 30 };
        RowDelta delta = table.newRowDelta();
        for (int idToDelete : idsToDelete) {
            String country = countryForId(idToDelete);
            LOGGER.info("[DELETE] Writing equality delete for id={} (partition country={})", idToDelete, country);
            DeleteFile deleteFile = writePartitionedEqualityDelete(table, templateV2, idToDelete, country);
            delta.addDeletes(deleteFile);
        }
        delta.commit();

        long snap3 = table.currentSnapshot().snapshotId();
        TestConstants.Iceberg.snapshot3TimestampDateValue = LocalDate.now(ZoneOffset.UTC).plusDays(1).toString();
        LOGGER.info("[DELETE] Committed RowDelta snapshotId={} snapshotTimestamp={}", snap3,
                table.currentSnapshot().timestampMillis());
        printSnapshotSummary(table);
        validateCountsWithIceberg(table);
        LOGGER.info("[FINISH] snapshots: snap1={}, snap2={}, snap3={}", snap1, snap2, snap3);
    }

    private static void writeAllTypesTable(NessieCatalog catalog) throws Exception {
        Table allTypesTable =
                createTable(catalog, ALL_TYPES_TABLE_ID, buildAllTypesSchema(), PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={}", allTypesTable.name());
        LOGGER.info("[TABLE] location={}", allTypesTable.location());
        LOGGER.info("[TABLE] spec={}", allTypesTable.spec());
        LOGGER.info("[TABLE] schema(all_types)={}", allTypesTable.schema());
        writeAllTypesData(allTypesTable);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Creates a dedicated table covering every Iceberg Variant PhysicalType")
    private static void writeAllTypesVariantTable(org.apache.iceberg.nessie.NessieCatalog catalog) throws Exception {
        Table variantTable = createTable(catalog, ALL_TYPES_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={}", variantTable.name());
        LOGGER.info("[TABLE] location={}", variantTable.location());
        LOGGER.info("[TABLE] schema(all_types_variant)={}", variantTable.schema());
        writeAllTypesVariantData(variantTable);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Schema for the dedicated Variant-coverage table: an id plus a single VARIANT column")
    private static Schema buildAllTypesVariantSchema() {
        return new Schema(required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "variant_field", Types.VariantType.get()));
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Writes rows isolating: (1) an object covering every supported Variant PhysicalType, (2) a fully null Variant column, (3) a top-level Variant array, (4) a top-level Variant string, (5) a top-level Variant NULL value distinct from a null column, (6-24) every remaining PhysicalType as a bare top-level (non-object-nested) Variant value, (25) a string over the 63-byte short-string encoding threshold")
    private static void writeAllTypesVariantData(Table table) throws Exception {
        GenericRecord template = GenericRecord.create(table.schema());
        List<Record> records = new ArrayList<>();

        Record allTypesRow = template.copy();
        allTypesRow.setField("id", 1);
        allTypesRow.setField("variant_field", buildVariantAllPhysicalTypes());
        records.add(allTypesRow);

        Record nullColumnRow = template.copy();
        nullColumnRow.setField("id", 2);
        nullColumnRow.setField("variant_field", null);
        records.add(nullColumnRow);

        Record topLevelArrayRow = template.copy();
        topLevelArrayRow.setField("id", 3);
        org.apache.iceberg.variants.ValueArray topLevelArray = Variants.array();
        topLevelArray.add(Variants.of(10));
        topLevelArray.add(Variants.of("twenty"));
        topLevelArray.add(Variants.of(true));
        topLevelArrayRow.setField("variant_field", Variant.of(Variants.emptyMetadata(), topLevelArray));
        records.add(topLevelArrayRow);

        Record topLevelStringRow = template.copy();
        topLevelStringRow.setField("id", 4);
        topLevelStringRow.setField("variant_field",
                Variant.of(Variants.emptyMetadata(), Variants.of("top level scalar string")));
        records.add(topLevelStringRow);

        Record topLevelNullValueRow = template.copy();
        topLevelNullValueRow.setField("id", 5);
        topLevelNullValueRow.setField("variant_field", Variant.of(Variants.emptyMetadata(), Variants.ofNull()));
        records.add(topLevelNullValueRow);

        int nextId = 6;
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of(true));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of(false));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of((byte) 5));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of((short) 1000));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of(42));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of(9223372036854775807L));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of(3.14f));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of(2.718281828459045d));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of(new BigDecimal("3.14")));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.of(new BigDecimal("123456789.123")));
        nextId = addTopLevelScalarRow(template, records, nextId,
                Variants.of(new BigDecimal("123456789012345678901.123456789")));
        nextId = addTopLevelScalarRow(template, records, nextId,
                Variants.of(ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 })));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.ofDate(19723));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.ofTime(37230000000L));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.ofTimestamptz(1707000000000000L));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.ofIsoTimestampntz("2024-02-04T12:00:00"));
        nextId = addTopLevelScalarRow(template, records, nextId, Variants.ofTimestamptzNanos(1707000000000000000L));
        nextId = addTopLevelScalarRow(template, records, nextId,
                Variants.ofIsoTimestampntzNanos("2024-02-04T12:00:00"));
        nextId = addTopLevelScalarRow(template, records, nextId,
                Variants.ofUUID(UUID.fromString("550e8400-e29b-41d4-a716-446655440000")));
        // Longer than the 63-byte short-string encoding threshold, forcing SerializedPrimitive's STRING
        // case at read time instead of SerializedShortString.
        addTopLevelScalarRow(template, records, nextId, Variants.of("x".repeat(70)));

        String path = table.location() + "/data/all_types_variant.parquet";
        OutputFile out = table.io().newOutputFile(path);
        try (FileAppender<Record> writer =
                Parquet.write(out).schema(table.schema()).createWriterFunc(GenericParquetWriter::create).build()) {
            for (Record r : records) {
                writer.add(r);
            }
        }
        long bytes = out.toInputFile().getLength();
        LOGGER.info("[APPEND] file={} sizeBytes={} rows={}", path, bytes, records.size());
        DataFile df = DataFiles.builder(table.spec()).withPath(path).withRecordCount(records.size())
                .withFileSizeInBytes(bytes).withFormat(FileFormat.PARQUET).build();
        table.newAppend().appendFile(df).commit();
        LOGGER.info("[WRITE] all_types_variant table committed with {} record(s)", records.size());
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Appends a row whose variant_field is a bare top-level Variant value (not nested in an object); returns the next unused id")
    private static int addTopLevelScalarRow(GenericRecord template, List<Record> records, int id,
            org.apache.iceberg.variants.VariantValue value) {
        Record row = template.copy();
        row.setField("id", id);
        row.setField("variant_field", Variant.of(Variants.emptyMetadata(), value));
        records.add(row);
        return id + 1;
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "objectValue/arrayValue nest 3 levels deep (object->array->object and array->object) to exercise multi-level recursion")
    private static Variant buildVariantAllPhysicalTypes() {
        VariantMetadata metadata = Variants.metadata("nullValue", "boolTrueValue", "boolFalseValue", "int8Value",
                "int16Value", "int32Value", "int64Value", "floatValue", "doubleValue", "decimal4Value", "decimal8Value",
                "decimal16Value", "stringValue", "binaryValue", "dateValue", "timeValue", "timestamptzValue",
                "timestampntzValue", "timestamptzNanosValue", "timestampntzNanosValue", "uuidValue", "objectValue",
                "arrayValue", "nestedString", "nestedInt", "nestedArray", "deepInt", "deepBool");

        ShreddedObject root = Variants.object(metadata);
        root.put("nullValue", Variants.ofNull());
        root.put("boolTrueValue", Variants.of(true));
        root.put("boolFalseValue", Variants.of(false));
        root.put("int8Value", Variants.of((byte) 5));
        root.put("int16Value", Variants.of((short) 1000));
        root.put("int32Value", Variants.of(42));
        root.put("int64Value", Variants.of(9223372036854775807L));
        root.put("floatValue", Variants.of(3.14f));
        root.put("doubleValue", Variants.of(2.718281828459045d));
        root.put("decimal4Value", Variants.of(new BigDecimal("3.14")));
        root.put("decimal8Value", Variants.of(new BigDecimal("123456789.123")));
        root.put("decimal16Value", Variants.of(new BigDecimal("123456789012345678901.123456789")));
        root.put("stringValue", Variants.of("hello variant"));
        root.put("binaryValue", Variants.of(ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 })));
        root.put("dateValue", Variants.ofDate(19723));
        root.put("timeValue", Variants.ofTime(37230000000L));
        root.put("timestamptzValue", Variants.ofTimestamptz(1707000000000000L));
        root.put("timestampntzValue", Variants.ofIsoTimestampntz("2024-02-04T12:00:00"));
        root.put("timestamptzNanosValue", Variants.ofTimestamptzNanos(1707000000000000000L));
        root.put("timestampntzNanosValue", Variants.ofIsoTimestampntzNanos("2024-02-04T12:00:00"));
        root.put("uuidValue", Variants.ofUUID(UUID.fromString("550e8400-e29b-41d4-a716-446655440000")));

        ShreddedObject nested = Variants.object(metadata);
        nested.put("nestedString", Variants.of("inner"));
        nested.put("nestedInt", Variants.of(7));
        ShreddedObject deepObjectInArray = Variants.object(metadata);
        deepObjectInArray.put("deepInt", Variants.of(99));
        org.apache.iceberg.variants.ValueArray nestedObjectArray = Variants.array();
        nestedObjectArray.add(deepObjectInArray);
        nested.put("nestedArray", nestedObjectArray);
        root.put("objectValue", nested);

        ShreddedObject deepObjectInTopArray = Variants.object(metadata);
        deepObjectInTopArray.put("deepBool", Variants.of(true));
        org.apache.iceberg.variants.ValueArray nestedArray = Variants.array();
        nestedArray.add(Variants.of(1));
        nestedArray.add(Variants.of("two"));
        nestedArray.add(Variants.of(3.0d));
        nestedArray.add(deepObjectInTopArray);
        root.put("arrayValue", nestedArray);

        return Variant.of(metadata, root);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Dedicated minimal table for testing the variantDepth WITH-clause guard: a single row whose Variant is nested exactly 3 levels deep")
    private static void writeDepthTestVariantTable(org.apache.iceberg.nessie.NessieCatalog catalog) throws Exception {
        Table depthTestTable = createTable(catalog, DEPTH_TEST_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={}", depthTestTable.name());
        LOGGER.info("[TABLE] location={}", depthTestTable.location());
        LOGGER.info("[TABLE] schema(depth_test_variant)={}", depthTestTable.schema());
        writeDepthTestVariantData(depthTestTable);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Writes a single row whose Variant nests exactly 3 levels deep (object -> object -> scalar leaf), used to verify the variantDepth WITH-clause option rejects data nested deeper than the configured limit")
    private static void writeDepthTestVariantData(Table table) throws Exception {
        GenericRecord template = GenericRecord.create(table.schema());
        Record row = template.copy();
        row.setField("id", 1);
        row.setField("variant_field", buildDepthThreeVariant());

        String path = table.location() + "/data/depth_test_variant.parquet";
        OutputFile out = table.io().newOutputFile(path);
        try (FileAppender<Record> writer =
                Parquet.write(out).schema(table.schema()).createWriterFunc(GenericParquetWriter::create).build()) {
            writer.add(row);
        }
        long bytes = out.toInputFile().getLength();
        LOGGER.info("[APPEND] file={} sizeBytes={} rows=1", path, bytes);
        DataFile df = DataFiles.builder(table.spec()).withPath(path).withRecordCount(1).withFileSizeInBytes(bytes)
                .withFormat(FileFormat.PARQUET).build();
        table.newAppend().appendFile(df).commit();
        LOGGER.info("[WRITE] depth_test_variant table committed with 1 record");
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Builds a Variant nested exactly 3 levels deep: depth 1 is the outer object, depth 2 is the nested object under field \"a\", depth 3 is the scalar leaf under field \"b\"")
    private static Variant buildDepthThreeVariant() {
        VariantMetadata metadata = Variants.metadata("a", "b");
        ShreddedObject depthTwo = Variants.object(metadata);
        depthTwo.put("b", Variants.of(42));
        ShreddedObject depthOne = Variants.object(metadata);
        depthOne.put("a", depthTwo);
        return Variant.of(metadata, depthOne);
    }

    // Top-level object fields promoted to typed_value in the PARTIALLY shredded fixture; every other field stays in
    // the object-level residual value. Deliberately a mix (scalars, a date, a nested object) so the residual carries
    // scalars, decimals, timestamps, binary, uuid, and the top-level array — the exact path Iceberg 1.10.x corrupted
    // and 1.11.0 fixed (issue #15086 / PR #15087).
    private static final Set<String> PARTIAL_SHREDDED_FIELDS =
            Set.of("int32Value", "stringValue", "boolTrueValue", "doubleValue", "dateValue", "objectValue");

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Read-side FULL-shredding counterpart to IcebergVariantSerializedAndShreddedTest: writes the SAME multi-row variant fixture serialized (first half of ids) and FULLY shredded (second half, every field promoted to a typed sub-column), so an ORDER BY id query proves the read pipeline reconstructs identical values whether the column is serialized or fully shredded")
    private static void writeShreddedVariantTable(NessieCatalog catalog) throws Exception {
        Table table = createTable(catalog, SHREDDED_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={}", table.name());
        LOGGER.info("[TABLE] location={}", table.location());
        LOGGER.info("[TABLE] schema(shredded_variant)={}", table.schema());
        // The shredding schema is derived from the all-PhysicalType object (row 1) so every supported type gets a
        // typed sub-column. The serialized file (ids 1..N) and the shredded file (ids N+1..2N) carry the SAME rows, so
        // matching them across ids proves full shredding reconstructs each value identically to serialized.
        Set<String> allFields = new TreeSet<>();
        buildVariantAllPhysicalTypes().value().asObject().fieldNames().forEach(allFields::add);
        List<Variant> rows = variantFixtureRows();
        DataFile serialized =
                writeVariantEncodingFile(table, "shredded_variant_serialized.parquet", 1, rows, null, null);
        DataFile fullyShredded = writeVariantEncodingFile(table, "shredded_variant_full.parquet", rows.size() + 1,
                variantFixtureRows(), shreddingFunc(buildVariantAllPhysicalTypes(), null), allFields);
        table.newAppend().appendFile(serialized).appendFile(fullyShredded).commit();
        LOGGER.info("[WRITE] shredded_variant table committed with {} record(s)", 2 * rows.size());
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Dedicated PARTIAL-shredding read test: writes the SAME multi-row variant fixture serialized (first half of ids) and PARTIALLY shredded (second half, a subset of fields promoted to typed sub-columns and the rest kept in the object-level residual value), so an ORDER BY id query proves the read pipeline reconstructs identical values. Exercises the typed_value + residual merge that Iceberg 1.10.x corrupted and 1.11.0 fixed (issue #15086 / PR #15087)")
    private static void writePartiallyShreddedVariantTable(NessieCatalog catalog) throws Exception {
        Table table = createTable(catalog, PARTIALLY_SHREDDED_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={}", table.name());
        LOGGER.info("[TABLE] location={}", table.location());
        LOGGER.info("[TABLE] schema(partially_shredded_variant)={}", table.schema());
        List<Variant> rows = variantFixtureRows();
        DataFile serialized =
                writeVariantEncodingFile(table, "partially_shredded_variant_serialized.parquet", 1, rows, null, null);
        DataFile partiallyShredded = writeVariantEncodingFile(table, "partially_shredded_variant_partial.parquet",
                rows.size() + 1, variantFixtureRows(),
                shreddingFunc(buildVariantAllPhysicalTypes(), PARTIAL_SHREDDED_FIELDS), PARTIAL_SHREDDED_FIELDS);
        table.newAppend().appendFile(serialized).appendFile(partiallyShredded).commit();
        LOGGER.info("[WRITE] partially_shredded_variant table committed with {} record(s)", 2 * rows.size());
    }

    // The per-bucket shredded variant shape shared by manyFilesVariant and deletesVariant. Several sub-field types so
    // predicate pushdown can be exercised per type end to end (int, string, double, boolean and a nested int), not
    // just on the int bucket.
    private static final VariantMetadata BUCKET_VARIANT_METADATA =
            Variants.metadata("bucket", "name", "ratio", "flag", "obj", "deep");

    /** The variant for bucket {@code i}: bucket=i, name="f"+i, ratio=i*1.5, flag=(i%2==0), obj.deep=i. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Extracted from writeManyFilesVariantTable so the deletes fixture writes byte-identical per-bucket variants")
    private static Variant bucketVariant(int i) {
        ShreddedObject nested = Variants.object(BUCKET_VARIANT_METADATA);
        nested.put("deep", Variants.of(i));
        ShreddedObject obj = Variants.object(BUCKET_VARIANT_METADATA);
        obj.put("bucket", Variants.of(i));
        obj.put("name", Variants.of("f" + i));
        obj.put("ratio", Variants.of(i * 1.5d));
        obj.put("flag", Variants.of(i % 2 == 0));
        obj.put("obj", nested);
        return Variant.of(BUCKET_VARIANT_METADATA, obj);
    }

    /**
     * Writes one fully-shredded data file holding the given {@code buckets}, one row each in the order given, and
     * stages it on {@code append} with real metrics attached, so the file's manifest entry carries a narrow bound per
     * shredded sub-field. Fixtures pass a single bucket so file-level pruning is observable: the number of objects
     * processed equals the number of data files a predicate could not skip. Several buckets in one file is for tests
     * that need a delete to remove some rows of a file without emptying it.
     *
     * @return the data file's path, so a deletion vector can reference it
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Extracted from writeManyFilesVariantTable so manyFilesVariant and deletesVariant share one file writer")
    private static String appendShreddedBucketFile(Table table, AppendFiles append, VariantShreddingFunction shredder,
            GenericRecord template, String filePrefix, int... buckets) throws Exception {
        String path = table.location() + "/data/" + filePrefix + buckets[0] + ".parquet";
        OutputFile out = table.io().newOutputFile(path);
        try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc(shredder).build()) {
            for (int bucket : buckets) {
                Record row = template.copy();
                row.setField("id", bucket + 1);
                row.setField("variant_field", bucketVariant(bucket));
                writer.add(row);
            }
        }
        long bytes = out.toInputFile().getLength();
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
        append.appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(bytes).withMetrics(metrics).build());
        return path;
    }

    /**
     * One table whose files do NOT agree on which variant sub-fields are shredded — the shape schema evolution
     * produces when a table is written over time and the shredding config changes underneath it.
     * <p>
     * The first half of the files shred every sub-field, so each carries a narrow bound for {@code bucket}. The second
     * half shred only {@code name}, so their {@code bucket} lives in the residual blob and has <b>no bound at all</b>.
     * A predicate on {@code bucket} therefore meets both kinds of file in a single scan.
     * <p>
     * The risk this pins down is losing a row: bounds must never be treated as authoritative for a file that does not
     * publish them. A file with no bound for the path must be read, not skipped — while the files that DO publish
     * bounds must still prune, or the test would pass for the trivial reason that nothing prunes at all. The two
     * queries in the suite assert both halves of that.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Files disagreeing on which variant sub-fields are shredded, so a predicate meets files with bounds and files without in one scan; guards against treating an absent bound as proof of no match")
    private static void writeMixedShreddingVariantTable(NessieCatalog catalog) throws Exception {
        Table table = createTable(catalog, MIXED_SHREDDING_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={} location={}", table.name(), table.location());
        VariantShreddingFunction full = shreddingFunc(bucketVariant(0), null);
        // Only "name" is promoted to a typed sub-column here, so "bucket" falls into the residual and gets no bounds.
        VariantShreddingFunction nameOnly = shreddingFunc(bucketVariant(0), Set.of("name"));
        GenericRecord template = GenericRecord.create(table.schema());
        AppendFiles append = table.newAppend();
        for (int i = 0; i < MIXED_SHREDDING_SHREDDED_FILES; i++) {
            appendShreddedBucketFile(table, append, full, template, "mixed_full_", i);
        }
        for (int i = MIXED_SHREDDING_SHREDDED_FILES; i < MIXED_SHREDDING_FILE_COUNT; i++) {
            appendShreddedBucketFile(table, append, nameOnly, template, "mixed_partial_", i);
        }
        append.commit();
        LOGGER.info(
                "[WRITE] mixedShreddingVariant committed: {} fully shredded files (buckets 0..{}) and {} files "
                        + "shredding only name (buckets {}..{}), so bucket has bounds in the first group only",
                MIXED_SHREDDING_SHREDDED_FILES, MIXED_SHREDDING_SHREDDED_FILES - 1,
                MIXED_SHREDDING_FILE_COUNT - MIXED_SHREDDING_SHREDDED_FILES, MIXED_SHREDDING_SHREDDED_FILES,
                MIXED_SHREDDING_FILE_COUNT - 1);
    }

    /**
     * A variant holding <b>both</b> a sub-field literally named {@code "a.b"} and a nested {@code a} -> {@code b}, with
     * different values, in one fully-shredded single-file table.
     * <p>
     * These two are indistinguishable once a filter's reference name is built: {@code IcebergTableFilterBuilder} joins
     * the field-access path with {@code String.join(".", ..)}, so both arrive as the string
     * {@code "variant_field.a.b"}. A rewrite that reads that as nesting would prune against the WRONG sub-field's
     * bounds, and because the two values differ, doing so skips this table's only data file and returns no rows at all.
     * The suite asserts the answer, so a mis-target is a visible wrong result rather than a silent inefficiency.
     * <p>
     * Values are chosen so the two readings disagree and the wrong one prunes everything away: the literal
     * {@code "a.b"} is 5, the nested {@code a.b} is 99, so bounds for the nested path cannot contain 5.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Fixture probing whether a sub-field name containing a dot can be confused with nesting: holds literal \"a.b\"=5 alongside nested a.b=99 so pruning against the wrong path skips the only file and yields zero rows")
    private static void writeDottedFieldNameVariantTable(NessieCatalog catalog) throws Exception {
        Table table = createTable(catalog, DOTTED_FIELD_NAME_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={} location={}", table.name(), table.location());
        VariantMetadata meta = Variants.metadata("a.b", "a", "b");
        ShreddedObject nested = Variants.object(meta);
        nested.put("b", Variants.of(99));
        ShreddedObject obj = Variants.object(meta);
        obj.put("a.b", Variants.of(5));
        obj.put("a", nested);
        Variant variant = Variant.of(meta, obj);

        VariantShreddingFunction shredder = shreddingFunc(variant, null);
        GenericRecord template = GenericRecord.create(table.schema());
        String path = table.location() + "/data/dotted_field_name.parquet";
        OutputFile out = table.io().newOutputFile(path);
        try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc(shredder).build()) {
            Record row = template.copy();
            row.setField("id", 1);
            row.setField("variant_field", variant);
            writer.add(row);
        }
        long bytes = out.toInputFile().getLength();
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(bytes).withMetrics(metrics).build()).commit();
        LOGGER.info("[WRITE] dottedFieldNameVariant committed: literal \"a.b\"=5, nested a.b=99");
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Writes one shredded, single-row data file per bucket value (0..N-1) with bucket+name promoted to typed sub-columns and real metrics attached, so each file's manifest carries a narrow per-subfield bound. A predicate on variant_field.bucket then prunes almost every file via Iceberg's manifest evaluator (exercises variant sub-field predicate pushdown / file skipping)")
    private static void writeManyFilesVariantTable(NessieCatalog catalog) throws Exception {
        Table table = createTable(catalog, MANY_FILES_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={} location={}", table.name(), table.location());
        // One fully-shredded typed_value schema derived from bucket 0's shape, reused for every file, so all files
        // shred the same way and each carries its own narrow bounds.
        VariantShreddingFunction shredder = shreddingFunc(bucketVariant(0), null);
        GenericRecord template = GenericRecord.create(table.schema());
        AppendFiles append = table.newAppend();
        for (int i = 0; i < MANY_FILES_VARIANT_FILE_COUNT; i++) {
            appendShreddedBucketFile(table, append, shredder, template, "many_", i);
        }
        append.commit();
        LOGGER.info("[WRITE] manyFilesVariant committed with {} single-row shredded files",
                MANY_FILES_VARIANT_FILE_COUNT);
    }

    /**
     * The same per-bucket shredded layout as {@link #writeManyFilesVariantTable}, plus <b>equality deletes</b>, so the
     * variant read path is exercised with deletes present — a combination nothing else in either harness covers.
     * <p>
     * Two things are deliberately true of this fixture:
     * <ul>
     * <li><b>Every scan task has deletes attached.</b> An equality delete applies to all data files with a lower
     * sequence number in the same partition, so committing the deletes in a later snapshot than the appends means no
     * task takes the no-deletes branch. That forces the whole table through {@code DeleteFilter.requiredSchema()} —
     * the widened-projection read path where variant sub-path <em>projection</em> pushdown is deliberately skipped, so
     * these queries prove the fallback reconstructs variant sub-fields correctly.</li>
     * <li><b>Predicate pushdown is still active.</b> File skipping happens during scan planning and does not look at
     * deletes, which is sound because deletes only ever remove rows. So a query here must both prune to the right file
     * set and drop the deleted rows.</li>
     * </ul>
     * Equality (not positional) deletes on purpose: these tables are format v3, where position deletes are superseded
     * by deletion vectors, and equality deletes are the mechanism this harness already exercises on {@code users}.
     * Deletion vectors are covered separately, by {@code writeDeletionVectorVariantTable} and the
     * {@code variant-deletion-vectors} suite: the two delete kinds differ in scope, so both are needed. An equality
     * delete is partition-scoped and attaches to every file in the partition, disabling projection pushdown across the
     * whole scan; a deletion vector is file-scoped, so pruned and fallback reads interleave within one scan.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Shredded per-bucket variant table with equality deletes committed in a later snapshot, so every scan task carries deletes and the delete-aware read path (requiredSchema widening) is exercised together with variant sub-field file pruning")
    private static void writeDeletesVariantTable(NessieCatalog catalog) throws Exception {
        Table table = createTable(catalog, DELETES_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={} location={}", table.name(), table.location());
        VariantShreddingFunction shredder = shreddingFunc(bucketVariant(0), null);
        GenericRecord template = GenericRecord.create(table.schema());
        AppendFiles append = table.newAppend();
        for (int i = 0; i < DELETES_VARIANT_FILE_COUNT; i++) {
            appendShreddedBucketFile(table, append, shredder, template, "del_", i);
        }
        append.commit();

        RowDelta delta = table.newRowDelta();
        for (int bucket : DELETES_VARIANT_DELETED_BUCKETS) {
            // Row ids are bucket + 1 (see appendShreddedBucketFile).
            delta.addDeletes(writeUnpartitionedEqualityDelete(table, bucket + 1));
        }
        delta.commit();
        LOGGER.info("[WRITE] deletesVariant committed with {} single-row shredded files; deleted buckets={}",
                DELETES_VARIANT_FILE_COUNT, Arrays.toString(DELETES_VARIANT_DELETED_BUCKETS));
    }

    /**
     * Writes an equality delete on {@code id} for an unpartitioned table. The delete file's row schema is just
     * {@code id} rather than the whole table schema, so the variant column never appears in a delete file — the
     * equality comparison only needs the key.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Unpartitioned counterpart to writePartitionedEqualityDelete, with an id-only delete row schema so no variant column is written into the delete file")
    private static DeleteFile writeUnpartitionedEqualityDelete(Table table, int idToDelete) throws Exception {
        Schema deleteRowSchema = table.schema().select("id");
        String delPath = table.location() + "/deletes/delete-eq-" + idToDelete + ".parquet";
        OutputFile delOut = table.io().newOutputFile(delPath);
        EqualityDeleteWriter<Record> delWriter = Parquet.writeDeletes(delOut).forTable(table)
                .rowSchema(deleteRowSchema).withSpec(table.spec())
                .equalityFieldIds(Collections.singletonList(table.schema().findField("id").fieldId()))
                .createWriterFunc(GenericParquetWriter::create).buildEqualityWriter();
        try (delWriter) {
            Record d = GenericRecord.create(deleteRowSchema);
            d.setField("id", idToDelete);
            delWriter.write(d);
        }
        DeleteFile deleteFile = delWriter.toDeleteFile();
        LOGGER.info("[DELETE FILE] location={} recordCount={} equalityFieldIds={}", deleteFile.location(),
                deleteFile.recordCount(), deleteFile.equalityFieldIds());
        return deleteFile;
    }

    /**
     * A shredded per-bucket variant table whose deletes are <b>deletion vectors</b> — the format-v3 replacement for
     * position delete files: a roaring bitmap of deleted row positions for a single data file, stored as a Puffin blob.
     * <p>
     * This covers what {@link #writeDeletesVariantTable} structurally cannot, because a DV is <em>file-scoped</em>
     * where an equality delete is partition-scoped:
     * <ul>
     * <li><b>One scan mixes both read paths.</b> Only files that have a DV carry deletes, so within a single query some
     * tasks take the no-deletes branch (variant projection pushdown active) while others take the delete-aware branch.
     * The equality-delete fixture puts every task on the delete branch and can never produce this mix.</li>
     * <li><b>A delete that removes only part of a file.</b> The multi-row file keeps two of its three rows, so the
     * delete-aware read must still return correctly reconstructed variant sub-fields rather than nothing. Position
     * deletes also make {@code DeleteFilter.requiredSchema()} add the synthetic {@code _pos} metadata column — a
     * widening equality deletes never trigger, and the one that would defeat the schema clipper if variant projection
     * pushdown were ever enabled on the delete path.</li>
     * </ul>
     * No production code here is delete-kind aware: the read path gates on whether a task has any deletes at all and
     * hands the rest to Iceberg, whose delete loader reads DVs itself. This fixture is what proves that.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Deletion-vector (format v3) counterpart to the equality-delete fixture: DVs are file-scoped, so only some files carry deletes and one scan mixes the pruned no-deletes read with the delete-aware read; the multi-row file proves a DV removing only part of a file still reconstructs the surviving rows")
    private static void writeDeletionVectorVariantTable(NessieCatalog catalog) throws Exception {
        Table table = createTable(catalog, DELETION_VECTOR_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={} location={}", table.name(), table.location());
        VariantShreddingFunction shredder = shreddingFunc(bucketVariant(0), null);
        GenericRecord template = GenericRecord.create(table.schema());
        AppendFiles append = table.newAppend();
        String emptiedFile = null;
        for (int i = 0; i < DELETION_VECTOR_VARIANT_FILE_COUNT; i++) {
            String path = appendShreddedBucketFile(table, append, shredder, template, "dv_", i);
            if (i == DELETION_VECTOR_DELETED_BUCKET) {
                emptiedFile = path;
            }
        }
        String multiRowFile = appendShreddedBucketFile(table, append, shredder, template, "dv_multi_",
                DELETION_VECTOR_MULTI_ROW_BUCKETS);
        String multiDeleteFile = appendShreddedBucketFile(table, append, shredder, template, "dv_multidel_",
                DELETION_VECTOR_MULTI_DELETE_BUCKETS);
        append.commit();

        // A position is the row's index within its own data file, so the multi-row file's middle bucket is position 1.
        RowDelta delta = table.newRowDelta();
        delta.addDeletes(writeDeletionVector(table, emptiedFile, "single", 0L));
        delta.addDeletes(writeDeletionVector(table, multiRowFile, "multi", 1L));
        // Two positions in ONE vector, leaving the rows between and after them: the only step that exercises a
        // deletion vector holding more than a single position.
        delta.addDeletes(writeDeletionVector(table, multiDeleteFile, "multidel", 0L, 2L));
        delta.commit();
        LOGGER.info(
                "[WRITE] deletionVectorVariant committed: {} single-row files (bucket {} emptied by a DV) plus one "
                        + "multi-row file {} with position 1 DV-deleted",
                DELETION_VECTOR_VARIANT_FILE_COUNT, DELETION_VECTOR_DELETED_BUCKET,
                Arrays.toString(DELETION_VECTOR_MULTI_ROW_BUCKETS));
    }

    /**
     * Writes a deletion vector removing {@code positions} from {@code dataFilePath} and returns it. Iceberg writes the
     * bitmap as a Puffin blob; the writer's second constructor argument is the "load this data file's existing DV"
     * callback, and there is none here. Format v3 allows at most one DV per data file, which is asserted.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Writes a Puffin deletion vector for the given row positions of one data file")
    private static DeleteFile writeDeletionVector(Table table, String dataFilePath, String dvName, long... positions)
            throws Exception {
        String dvPath = table.location() + "/deletes/dv-" + dvName + ".puffin";
        DVFileWriter dvWriter = new BaseDVFileWriter(() -> table.io().newOutputFile(dvPath), path -> null);
        try (dvWriter) {
            for (long position : positions) {
                dvWriter.delete(dataFilePath, position, table.spec(), null);
            }
        }
        List<DeleteFile> written = dvWriter.result().deleteFiles();
        Assert.assertEquals("v3 allows at most one deletion vector per data file", 1, written.size());
        DeleteFile dv = written.get(0);
        LOGGER.info("[DV] location={} referencedDataFile={} recordCount={}", dv.location(), dv.referencedDataFile(),
                dv.recordCount());
        return dv;
    }

    /**
     * A single data file with many Parquet row groups, so a read actually iterates the row-group loop instead of
     * consuming one group and stopping. Every other fixture writes one row group per file, which leaves that loop —
     * and the row-group skipping decisions layered on it — untested.
     * <p>
     * Two things this makes reachable:
     * <ul>
     * <li><b>Row-group boundaries.</b> Thousands of rows spread over many groups will expose a reader that drops or
     * repeats rows when advancing between them. The queries assert aggregates over the whole file, so a single
     * duplicated or missing row changes the answer.</li>
     * <li><b>Row-group skipping.</b> A predicate on the real {@code id} column binds against the Iceberg schema and
     * reaches the metrics row-group filter, so groups outside its range are skipped rather than decoded. A predicate
     * on a shredded variant sub-field cannot do this — {@code iceberg-parquet} has no extract-term support in its
     * row-group filters — which is why this fixture filters on {@code id}.</li>
     * </ul>
     * No deletes, so the read takes the variant-pruned path rather than the delete-aware one.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Single data file written with a tiny Parquet row-group size so it holds many row groups, making the reader's row-group loop and its skipping decisions reachable; every other fixture file has one row group")
    private static void writeManyRowGroupsVariantTable(NessieCatalog catalog) throws Exception {
        Table table = createTable(catalog, MANY_ROW_GROUPS_VARIANT_TABLE_ID, buildAllTypesVariantSchema(),
                PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={} location={}", table.name(), table.location());
        VariantShreddingFunction shredder = shreddingFunc(bucketVariant(0), null);
        GenericRecord template = GenericRecord.create(table.schema());
        String path = table.location() + "/data/many_row_groups.parquet";
        OutputFile out = table.io().newOutputFile(path);
        try (FileAppender<Record> writer =
                Parquet.write(out).schema(table.schema())
                        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
                                Integer.toString(MANY_ROW_GROUPS_ROW_GROUP_BYTES))
                        .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc(shredder).build()) {
            for (int i = 0; i < MANY_ROW_GROUPS_VARIANT_ROW_COUNT; i++) {
                Record row = template.copy();
                row.setField("id", i + 1);
                row.setField("variant_field", bucketVariant(i));
                writer.add(row);
            }
        }
        long bytes = out.toInputFile().getLength();
        assertMultipleRowGroups(table, path);
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(bytes).withMetrics(metrics).build()).commit();
        LOGGER.info("[WRITE] manyRowGroupsVariant committed: 1 file, {} rows", MANY_ROW_GROUPS_VARIANT_ROW_COUNT);
    }

    /**
     * Fails the fixture if the file ended up with a single row group. Without this the suite would still pass while
     * testing nothing it was written for, because a one-row-group file never enters the loop under test.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Guards the row-groups fixture: fails if the file collapsed to a single row group, which would silently void the suite")
    private static void assertMultipleRowGroups(Table table, String path) throws Exception {
        int rowGroups;
        try (ParquetFileReader reader = ParquetFileReader.open(parquetInput(table.io().newInputFile(path)))) {
            rowGroups = reader.getFooter().getBlocks().size();
        }
        LOGGER.info("[VERIFY] file={} rowGroups={}", path, rowGroups);
        Assert.assertTrue(
                "expected more than one Parquet row group so the reader's row-group loop is exercised, got " + rowGroups
                        + "; raise MANY_ROW_GROUPS_VARIANT_ROW_COUNT or lower MANY_ROW_GROUPS_ROW_GROUP_BYTES",
                rowGroups > 1);
    }

    /**
     * A VARIANT nested inside a struct, alongside an ordinary string sibling.
     * <p>
     * Iceberg cannot resolve a name through a variant at any depth, so neither {@code st.v} nor {@code st.v.bucket}
     * binds — meaning a predicate on the nested variant can never be pushed down. What this fixture establishes is
     * that such a query still <em>works</em>: the values are read and reconstructed, the engine applies the predicate
     * to the rows, and the answer is correct. The sibling {@code st.label} is the control — a predicate on it must
     * still push down, proving a variant next to it does not poison pruning for the whole struct.
     * <p>
     * Written serialized rather than shredded on purpose: sub-field bounds are irrelevant here because nothing can be
     * pruned on a nested variant either way, and serialized keeps the fixture honest about what is being tested.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Table with a VARIANT nested inside a struct plus an ordinary sibling column, to establish that queries on a nested variant still return correct results (no pushdown possible) and that pruning on the sibling is unaffected")
    private static void writeNestedVariantInStructTable(NessieCatalog catalog) throws Exception {
        Schema schema = new Schema(required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "st",
                        Types.StructType.of(Types.NestedField.optional(3, "v", Types.VariantType.get()),
                                Types.NestedField.optional(4, "label", Types.StringType.get()))));
        Table table = createTable(catalog, NESTED_VARIANT_TABLE_ID, schema, PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={} location={}", table.name(), table.location());

        GenericRecord template = GenericRecord.create(table.schema());
        GenericRecord structTemplate = GenericRecord.create(table.schema().findField("st").type().asStructType());
        // The nested variant is SHREDDED, so its sub-fields carry manifest bounds. Without that, a predicate on
        // st.v.bucket has nothing to prune against and this fixture could not tell "we decline to push it" apart from
        // "we push it but there are no statistics".
        VariantShreddingFunction nestedShredder = shreddingFunc(bucketVariant(0), null);
        // One row per file, so pruning is observable through EITHER path: st.label has a distinct bound per file,
        // and the shredded st.v sub-fields have their own bounds, so a predicate on the nested variant prunes too.
        AppendFiles append = table.newAppend();
        for (int i = 0; i < NESTED_VARIANT_ROW_COUNT; i++) {
            String path = table.location() + "/data/nested_variant_" + i + ".parquet";
            OutputFile out = table.io().newOutputFile(path);
            Record struct = structTemplate.copy();
            struct.setField("v", bucketVariant(i));
            struct.setField("label", "L" + i);
            Record row = template.copy();
            row.setField("id", i + 1);
            row.setField("st", struct);
            try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc(nestedShredder).build()) {
                writer.add(row);
            }
            long bytes = out.toInputFile().getLength();
            Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
            append.appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(bytes).withMetrics(metrics).build());
        }
        append.commit();
        LOGGER.info("[WRITE] nestedVariantInStruct committed with {} single-row file(s)", NESTED_VARIANT_ROW_COUNT);
    }

    /**
     * A <b>partitioned</b> table with a shredded VARIANT column. Every other variant fixture is unpartitioned, so
     * nothing else covers partition pruning and variant sub-field pruning acting on the same scan.
     * <p>
     * Buckets 0..11 go one row per file, grouped three consecutive buckets to a partition ({@code grp} = g0..g3), so
     * each mechanism prunes a different amount and the two can be told apart: a predicate on {@code grp} alone leaves
     * one partition's three files, a variant sub-field predicate alone leaves one file across all partitions, and both
     * together still leave exactly one.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Partitioned table with a shredded VARIANT, the only fixture covering partition pruning together with variant sub-field pruning")
    private static void writePartitionedVariantTable(NessieCatalog catalog) throws Exception {
        Schema schema = new Schema(required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "grp", Types.StringType.get()),
                Types.NestedField.optional(3, "variant_field", Types.VariantType.get()));
        Table table = catalog.buildTable(PARTITIONED_VARIANT_TABLE_ID, schema)
                .withPartitionSpec(PartitionSpec.builderFor(schema).identity("grp").build())
                .withProperty(TableProperties.FORMAT_VERSION, "3").create();
        LOGGER.info("[TABLE] name={} location={} spec={}", table.name(), table.location(), table.spec());

        VariantShreddingFunction shredder = shreddingFunc(bucketVariant(0), null);
        GenericRecord template = GenericRecord.create(table.schema());
        AppendFiles append = table.newAppend();
        for (int i = 0; i < PARTITIONED_VARIANT_FILE_COUNT; i++) {
            String group = "g" + (i / PARTITIONED_VARIANT_GROUP_SIZE);
            PartitionData partitionData = new PartitionData(table.spec().partitionType());
            partitionData.set(0, group);
            String path = table.location() + "/data/" + group + "_" + i + ".parquet";
            OutputFile out = table.io().newOutputFile(path);
            Record row = template.copy();
            row.setField("id", i + 1);
            row.setField("grp", group);
            row.setField("variant_field", bucketVariant(i));
            try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc(shredder).build()) {
                writer.add(row);
            }
            long bytes = out.toInputFile().getLength();
            Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
            append.appendFile(DataFiles.builder(table.spec()).withPath(path).withPartition(partitionData)
                    .withFormat(FileFormat.PARQUET).withFileSizeInBytes(bytes).withMetrics(metrics).build());
        }
        append.commit();
        LOGGER.info("[WRITE] partitionedVariant committed with {} single-row files across {} partitions",
                PARTITIONED_VARIANT_FILE_COUNT, PARTITIONED_VARIANT_FILE_COUNT / PARTITIONED_VARIANT_GROUP_SIZE);
    }

    /**
     * <b>Two</b> shredded VARIANT columns in one table, each with its own shape. {@code VariantProjectionPlan} is a map
     * keyed by column and {@code VariantPredicateRewriter} resolves the root column per predicate, so both should be
     * handled independently — but every other fixture has exactly one variant column, so nothing tested it. Pruning
     * must work through either column, and projecting one must not drag the other's sub-columns along.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Table with two independent shredded VARIANT columns, each with its own shredding shape, to cover the per-column projection plan and rewriter root resolution")
    private static void writeTwoVariantsTable(NessieCatalog catalog) throws Exception {
        Schema schema = new Schema(required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "v1", Types.VariantType.get()),
                Types.NestedField.optional(3, "v2", Types.VariantType.get()));
        Table table = createTable(catalog, TWO_VARIANTS_TABLE_ID, schema, PartitionSpec.unpartitioned());
        LOGGER.info("[TABLE] name={} location={}", table.name(), table.location());

        VariantMetadata secondMeta = Variants.metadata("code", "score");
        // One typed_value per column: the shredding function is asked per field, so the two shapes stay distinct.
        Type firstTyped = toParquetTypedValue(bucketVariant(0));
        Type secondTyped = toParquetTypedValue(secondVariant(secondMeta, 0));
        VariantShreddingFunction shredder = (fieldId, name) -> "v1".equals(name) ? firstTyped : secondTyped;

        GenericRecord template = GenericRecord.create(table.schema());
        AppendFiles append = table.newAppend();
        for (int i = 0; i < TWO_VARIANTS_FILE_COUNT; i++) {
            String path = table.location() + "/data/two_" + i + ".parquet";
            OutputFile out = table.io().newOutputFile(path);
            Record row = template.copy();
            row.setField("id", i + 1);
            row.setField("v1", bucketVariant(i));
            row.setField("v2", secondVariant(secondMeta, i));
            try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc(shredder).build()) {
                writer.add(row);
            }
            long bytes = out.toInputFile().getLength();
            Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
            append.appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(bytes).withMetrics(metrics).build());
        }
        append.commit();
        LOGGER.info("[WRITE] twoVariants committed with {} single-row files", TWO_VARIANTS_FILE_COUNT);
    }

    /** The second variant column's shape: deliberately unlike bucketVariant so the two cannot be confused. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Second variant column's shape for the two-variant fixture")
    private static Variant secondVariant(VariantMetadata meta, int i) {
        ShreddedObject object = Variants.object(meta);
        object.put("code", Variants.of("c" + i));
        object.put("score", Variants.of(i * 2));
        return Variant.of(meta, object);
    }

    /** Iceberg's canonical typed_value for a value, via the package-private ParquetVariantUtil. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Reflective helper for Iceberg's canonical typed_value, shared by the two-variant shredding function")
    private static Type toParquetTypedValue(Variant value) throws Exception {
        Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        return (Type) toParquetSchema.invoke(null, value.value());
    }

    /**
     * The rows each encoding file carries. It deliberately varies the <b>top-level Variant PhysicalType</b> across
     * rows — an object, a bare int, a bare string, an array, a top-level null value, and a null column — rather than
     * only objects. The shredding schema is object-oriented (derived from the all-types object), so the non-object
     * rows fall to the residual {@code value} and must still reconstruct, exercising the mixed shredded/residual and
     * value-as-variant read paths across several records. Fresh instances per call so the same fixture can be written
     * to more than one file.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Multi-record fixture varying the top-level PhysicalType: all-types object, bare int, bare string, array, top-level null value, and a null column, so the shredded read path is exercised across heterogeneous rows (typed_value + residual + value-as-variant + null handling)")
    private static List<Variant> variantFixtureRows() {
        List<Variant> rows = new ArrayList<>();
        rows.add(buildVariantAllPhysicalTypes()); // object covering every PhysicalType (shreddable)
        rows.add(Variant.of(Variants.emptyMetadata(), Variants.of(123))); // bare int primitive
        rows.add(Variant.of(Variants.emptyMetadata(), Variants.of("top level string"))); // bare string primitive
        org.apache.iceberg.variants.ValueArray array = Variants.array();
        array.add(Variants.of(10));
        array.add(Variants.of("twenty"));
        array.add(Variants.of(true));
        rows.add(Variant.of(Variants.emptyMetadata(), array)); // top-level array
        rows.add(Variant.of(Variants.emptyMetadata(), Variants.ofNull())); // top-level null value (distinct from null column)
        rows.add(null); // null variant_field column
        return rows;
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Writes a multi-row Parquet data file (a null element writes a null variant column) using the given shredding function (null = serialized), then verifies the on-disk layout matches expectedShreddedFields (null = no typed_value) so a read test can never silently fall back to testing serialized data")
    private static DataFile writeVariantEncodingFile(Table table, String fileName, int startId, List<Variant> rowValues,
            VariantShreddingFunction shreddingFunc, Set<String> expectedShreddedFields) throws Exception {
        String path = table.location() + "/data/" + fileName;
        OutputFile out = table.io().newOutputFile(path);
        Parquet.WriteBuilder builder =
                Parquet.write(out).schema(table.schema()).createWriterFunc(GenericParquetWriter::create);
        if (shreddingFunc != null) {
            builder = builder.variantShreddingFunc(shreddingFunc);
        }
        GenericRecord template = GenericRecord.create(table.schema());
        int id = startId;
        try (FileAppender<Record> writer = builder.build()) {
            for (Variant value : rowValues) {
                Record row = template.copy();
                row.setField("id", id++);
                row.setField("variant_field", value);
                writer.add(row);
            }
        }
        long bytes = out.toInputFile().getLength();
        // Guard against silently testing serialized data: confirm the file's physical layout on disk matches the
        // intended encoding before any read test is allowed to rely on it.
        assertVariantLayoutOnDisk(table, path, expectedShreddedFields);
        LOGGER.info("[APPEND] file={} sizeBytes={} rows={} shreddedFields={}", path, bytes, rowValues.size(),
                expectedShreddedFields == null ? "(serialized)" : expectedShreddedFields);
        // Attach real metrics so the manifest carries per-subfield lower/upper bounds for the shredded VARIANT
        // column (Iceberg encodes them as a Variant object keyed by normalized JSON path). Without this, the
        // committed file has no bounds at all and Expressions.extract(..) file skipping can never prune, which
        // would make a pushdown test silently prove nothing.
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
        return DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET).withFileSizeInBytes(bytes)
                .withMetrics(metrics).build();
    }

    /**
     * Returns a {@link VariantShreddingFunction} for {@code value}. When {@code keep} is {@code null} it fully shreds —
     * every object field is promoted to a typed sub-column, leaving nothing in the object-level residual {@code value}.
     * When {@code keep} names a subset, only those fields are shredded and the rest stay in the residual value (partial
     * shredding). Per-field {@code typed_value} Parquet types come from Iceberg's own
     * {@code ParquetVariantUtil.toParquetSchema} (package-private, reached via reflection) so each of the ~23 Variant
     * PhysicalTypes is encoded exactly as Iceberg expects; the partial variant just keeps a subset of that group's
     * fields. Partial shredding round-trips correctly as of Iceberg 1.11.0 (the residual serialization bug in 1.10.x —
     * issue #15086 / PR #15087 — is fixed).
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Reflectively uses Iceberg's ParquetVariantUtil.toParquetSchema to build a canonical typed_value; keeps all fields (full shred) or a named subset (partial shred, rest to residual)")
    private static VariantShreddingFunction shreddingFunc(Variant value, Set<String> keep) throws Exception {
        Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        GroupType fullTyped = ((Type) toParquetSchema.invoke(null, value.value())).asGroupType();
        Type typedValue;
        if (keep == null) {
            typedValue = fullTyped;
        } else {
            org.apache.parquet.schema.Types.GroupBuilder<GroupType> builder =
                    org.apache.parquet.schema.Types.buildGroup(fullTyped.getRepetition());
            for (Type field : fullTyped.getFields()) {
                if (keep.contains(field.getName())) {
                    builder.addField(field);
                }
            }
            typedValue = builder.named(fullTyped.getName());
        }
        Type tv = typedValue;
        // Single variant column, so fieldId/name are ignored; the same typed_value applies.
        return (fieldId, name) -> tv;
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Reads the just-written file's Parquet footer through the table's own FileIO and asserts the variant column's typed_value holds exactly the expected shredded fields (null => serialized, no typed_value), so the fixture can never silently degrade to serialized or shred a different set than intended")
    private static void assertVariantLayoutOnDisk(Table table, String path, Set<String> expectedShreddedFields)
            throws Exception {
        GroupType variant;
        try (ParquetFileReader reader = ParquetFileReader.open(parquetInput(table.io().newInputFile(path)))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            variant = schema.getType("variant_field").asGroupType();
        }
        // Every encoding keeps the variant metadata and a value slot (residual for shredded, whole blob for serialized).
        Assert.assertTrue("variant must keep metadata", variant.containsField("metadata"));
        Assert.assertTrue("variant must keep a value blob", variant.containsField("value"));
        if (expectedShreddedFields == null) {
            Assert.assertFalse("serialized variant must NOT have typed_value", variant.containsField("typed_value"));
            LOGGER.info("[VERIFY] file={} confirmed serialized (no typed_value)", path);
            return;
        }
        Assert.assertTrue("shredded variant must have typed_value", variant.containsField("typed_value"));
        GroupType typedValue = variant.getType("typed_value").asGroupType();
        Set<String> actual = new TreeSet<>();
        typedValue.getFields().forEach(field -> actual.add(field.getName()));
        Assert.assertEquals("shredded typed_value fields on disk", new TreeSet<>(expectedShreddedFields), actual);
        LOGGER.info("[VERIFY] file={} confirmed shredded; typed sub-columns={}", path, actual);
    }

    /** Adapts an Iceberg {@link org.apache.iceberg.io.InputFile} to a Parquet InputFile for footer inspection. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Bridges Iceberg FileIO to a Parquet InputFile so the shredded-layout check reads the footer through the table's own IO instead of a separate S3 client")
    private static org.apache.parquet.io.InputFile parquetInput(org.apache.iceberg.io.InputFile in) {
        return new org.apache.parquet.io.InputFile() {
            @Override
            public long getLength() throws IOException {
                return in.getLength();
            }

            @Override
            public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
                org.apache.iceberg.io.SeekableInputStream delegate = in.newStream();
                return new org.apache.parquet.io.DelegatingSeekableInputStream(delegate) {
                    @Override
                    public long getPos() throws IOException {
                        return delegate.getPos();
                    }

                    @Override
                    public void seek(long newPos) throws IOException {
                        delegate.seek(newPos);
                    }
                };
            }
        };
    }

    private static org.apache.iceberg.nessie.NessieCatalog createNessieCatalog() throws Exception {
        LOGGER.info("[CATALOG] Initializing Nessie catalog at {}", NESSIE_URI);
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, "NESSIE");
        properties.put(CatalogProperties.CATALOG_IMPL, org.apache.iceberg.nessie.NessieCatalog.class.getName());
        properties.put("uri", NESSIE_URI);
        properties.put("warehouse", NESSIE_WAREHOUSE);
        properties.put(CatalogProperties.FILE_IO_IMPL, IcebergConstants.Aws.S3_FILE_IO);
        properties.put(AwsProperties.CLIENT_FACTORY, EnsureCloseAWSClientFactory.class.getName());
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL
                + AwsConstants.SERVICE_END_POINT_FIELD_NAME, s3Mock.getHttpEndpoint());
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL + AwsConstants.REGION_FIELD_NAME,
                MOCK_SERVER_REGION);
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL + "pathStyleAddressing", "true");
        properties.put(
                IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL + AwsConstants.ACCESS_KEY_ID_FIELD_NAME,
                TestConstants.S3_ACCESS_KEY_ID_DEFAULT);
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL
                + AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, TestConstants.S3_SECRET_ACCESS_KEY_DEFAULT);
        NessieUtils.setNessieCatalogProperties(properties);
        org.apache.iceberg.nessie.NessieCatalog catalog =
                (org.apache.iceberg.nessie.NessieCatalog) IcebergUtils.initializeCatalogOnly(properties);
        LOGGER.info("[CATALOG] Initialized Nessie catalog");
        return catalog;
    }

    private static Schema buildSchemaV1() {
        return new Schema(required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "country", Types.StringType.get()),
                Types.NestedField.optional(4, "booleanValue", Types.BooleanType.get()),
                Types.NestedField.optional(5, "longValue", Types.LongType.get()),
                Types.NestedField.optional(6, "floatValue", Types.FloatType.get()),
                Types.NestedField.optional(7, "doubleValue", Types.DoubleType.get()),
                Types.NestedField.optional(8, "price", Types.DecimalType.of(20, 5)),
                Types.NestedField.optional(9, "info_struct",
                        Types.StructType.of(Types.NestedField.required(201, "x", Types.IntegerType.get()),
                                Types.NestedField.optional(202, "y", Types.StringType.get()))),
                Types.NestedField.optional(10, "tags", Types.ListType.ofOptional(301, Types.StringType.get())),
                Types.NestedField.optional(11, "metrics",
                        Types.MapType.ofOptional(401, 402, Types.StringType.get(), Types.IntegerType.get())),
                Types.NestedField.optional(12, "uuidValue", Types.UUIDType.get()),
                Types.NestedField.optional(13, "fixed16", Types.FixedType.ofLength(16)),
                Types.NestedField.optional(14, "binary_value", Types.BinaryType.get()),
                Types.NestedField.optional(15, "event_date", Types.DateType.get()),
                Types.NestedField.optional(16, "event_time", Types.TimeType.get()));
    }

    private static Schema buildAllTypesSchema() {
        return new Schema(Types.NestedField.optional(1, "bool_field", Types.BooleanType.get()),
                Types.NestedField.optional(2, "byte_field", Types.IntegerType.get()),
                Types.NestedField.optional(3, "short_field", Types.IntegerType.get()),
                Types.NestedField.optional(4, "int_field", Types.IntegerType.get()),
                Types.NestedField.optional(5, "long_field", Types.LongType.get()),
                Types.NestedField.optional(6, "float_field", Types.FloatType.get()),
                Types.NestedField.optional(7, "double_field", Types.DoubleType.get()),
                Types.NestedField.optional(8, "decimal_field", Types.DecimalType.of(10, 4)),
                Types.NestedField.optional(9, "string_field", Types.StringType.get()),
                Types.NestedField.optional(10, "varchar_field", Types.StringType.get()),
                Types.NestedField.optional(11, "char_field", Types.StringType.get()),
                Types.NestedField.optional(12, "uuid_field", Types.UUIDType.get()),
                Types.NestedField.optional(13, "binary_field", Types.BinaryType.get()),
                Types.NestedField.optional(14, "fixed_field", Types.FixedType.ofLength(11)),
                Types.NestedField.optional(15, "date_field", Types.DateType.get()),
                Types.NestedField.optional(16, "time_field", Types.TimeType.get()),
                Types.NestedField.optional(17, "timestamp_field", Types.TimestampType.withZone()),
                Types.NestedField.optional(18, "timestamp_ntz_field", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(19, "timestamp_nano_field", Types.TimestampNanoType.withZone()),
                Types.NestedField.optional(20, "interval_ym_field", Types.IntegerType.get()),
                Types.NestedField.optional(21, "interval_dt_field", Types.LongType.get()),
                Types.NestedField.optional(22, "geometry_field", Types.BinaryType.get()),
                Types.NestedField.optional(23, "geography_field", Types.BinaryType.get()),
                Types.NestedField.optional(24, "struct_field",
                        Types.StructType.of(Types.NestedField.optional(241, "name", Types.StringType.get()),
                                Types.NestedField.optional(242, "age", Types.IntegerType.get()),
                                Types.NestedField.optional(243, "active", Types.BooleanType.get()))),
                Types.NestedField.optional(25, "list_field", Types.ListType.ofOptional(251, Types.StringType.get())),
                Types.NestedField.optional(26, "map_field",
                        Types.MapType.ofOptional(261, 262, Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.optional(27, "variant_field", Types.StringType.get()),
                Types.NestedField.optional(28, "unknown_field", Types.StringType.get()));
    }

    private static PartitionSpec buildPartitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema).identity("country").build();
    }

    private static Table createTable(org.apache.iceberg.nessie.NessieCatalog catalog, TableIdentifier id, Schema schema,
            PartitionSpec spec) {
        LOGGER.info("[CREATE] Creating table {} (format v3) ...", id);
        return catalog.buildTable(id, schema).withPartitionSpec(spec).withProperty(TableProperties.FORMAT_VERSION, "3")
                .create();
    }

    private static long writeSnapshot(Table table, GenericRecord template, int start, int end) throws Exception {
        Map<String, List<Record>> groups = groupRowsByCountry(table, template, start, end);
        int fileIndex = 0;
        int totalRows = 0;
        LOGGER.info("[SNAPSHOT] Writing rows {} → {}", start, end);
        for (Map.Entry<String, List<Record>> entry : groups.entrySet()) {
            String country = entry.getKey();
            List<Record> rows = entry.getValue();
            totalRows += rows.size();
            PartitionData pd = partition(table, country);
            String path = table.location() + "/data/" + start + "_" + end + "_p" + (fileIndex++) + ".parquet";
            DataFile df = writeDataFile(table, rows, path, pd);
            table.newAppend().appendFile(df).commit();
            Snapshot snap = table.currentSnapshot();
            LOGGER.info("[APPEND] snapshotId={} snapshotTimestamp={} country={} rows={} file={} sizeBytes={}",
                    snap.snapshotId(), snap.timestampMillis(), country, rows.size(), df.path(), df.fileSizeInBytes());
        }
        LOGGER.info("[SNAPSHOT] Done. snapshotId={} totalRows={}", table.currentSnapshot().snapshotId(), totalRows);
        return table.currentSnapshot().snapshotId();
    }

    private static Map<String, List<Record>> groupRowsByCountry(Table table, GenericRecord template, int start,
            int end) {
        Map<String, List<Record>> groups = new HashMap<>();
        OffsetDateTime base = OffsetDateTime.now(ZoneOffset.UTC);
        for (int i = start; i <= end; i++) {
            Record row = generateRow(table, i, template, base);
            String country = (String) row.getField("country");
            groups.computeIfAbsent(country, k -> new ArrayList<>()).add(row);
        }
        for (Map.Entry<String, List<Record>> entry : groups.entrySet()) {
            LOGGER.info("[GROUP] country={} rows={}", entry.getKey(), entry.getValue().size());
        }
        return groups;
    }

    private static Record generateRow(Table table, int i, GenericRecord template, OffsetDateTime baseTs) {
        Record r = template.copy();
        r.setField("id", i);
        r.setField("name", "User" + i);
        r.setField("country", countryForId(i));
        r.setField("event_date", LocalDate.now());
        r.setField("event_time", LocalTime.now());
        r.setField("booleanValue", i % 2 == 0);
        r.setField("longValue", i * 1000L);
        r.setField("floatValue", i * 0.1f);
        r.setField("doubleValue", i * 0.12345d);
        r.setField("uuidValue", UUID.randomUUID());
        r.setField("fixed16", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
        r.setField("binary_value", ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }));
        r.setField("price", new BigDecimal("123.45600"));
        GenericRecord struct =
                GenericRecord.create(((Types.StructType) table.schema().findField("info_struct").type()));
        struct.setField("x", i);
        struct.setField("y", "value-" + i);
        r.setField("info_struct", struct);
        r.setField("tags", Arrays.asList("tag1", "tag" + i));
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("count", i);
        metrics.put("val", i * 2);
        r.setField("metrics", metrics);
        return r;
    }

    private static DataFile writeDataFile(Table table, List<Record> rows, String path, PartitionData pd)
            throws Exception {
        OutputFile out = table.io().newOutputFile(path);
        DataFile dataFile;
        try (FileAppender<Record> writer =
                Parquet.write(out).schema(table.schema()).createWriterFunc(GenericParquetWriter::create).build()) {
            for (Record r : rows) {
                writer.add(r);
            }
        }
        long bytes = out.toInputFile().getLength();
        LOGGER.info("[APPEND] file={} sizeBytes={} rows={}", path, bytes, rows.size());
        dataFile = DataFiles.builder(table.spec()).withPath(path).withPartition(pd).withRecordCount(rows.size())
                .withFileSizeInBytes(bytes).withFormat(FileFormat.PARQUET).build();
        return dataFile;
    }

    private static DeleteFile writePartitionedEqualityDelete(Table table, GenericRecord template, int idToDelete,
            String country) throws Exception {
        PartitionData pd = partition(table, country);
        String delPath = table.location() + "/deletes/delete-eq-" + idToDelete + ".parquet";
        OutputFile delOut = table.io().newOutputFile(delPath);
        EqualityDeleteWriter<Record> delWriter = Parquet.writeDeletes(delOut).forTable(table).rowSchema(table.schema())
                .withSpec(table.spec()).withPartition(pd).equalityFieldIds(Collections.singletonList(1))
                .createWriterFunc(GenericParquetWriter::create).buildEqualityWriter();
        try (delWriter) {
            Record d = template.copy();
            d.setField("id", idToDelete);
            delWriter.write(d);
        }
        DeleteFile deleteFile = delWriter.toDeleteFile();
        LOGGER.info("[DELETE FILE] location={}", deleteFile.location());
        LOGGER.info("[DELETE FILE] content={}", deleteFile.content());
        LOGGER.info("[DELETE FILE] specId={}", deleteFile.specId());
        LOGGER.info("[DELETE FILE] partition={}", deleteFile.partition());
        LOGGER.info("[DELETE FILE] recordCount={}", deleteFile.recordCount());
        LOGGER.info("[DELETE FILE] equalityFieldIds={}", deleteFile.equalityFieldIds());
        return deleteFile;
    }

    private static PartitionData partition(Table table, String country) {
        PartitionData pd = new PartitionData(table.spec().partitionType());
        pd.set(0, country);
        return pd;
    }

    private static String countryForId(int id) {
        String[] countries = { "US", "UK", "DE", "FR", "SA", "JP" };
        return countries[id % countries.length];
    }

    private static void evolveSchemaAddColumn(Table table) {
        LOGGER.info("[SCHEMA EVOLUTION] Adding optional column: evolved_note (string)");
        table.updateSchema().addColumn("evolved_note", Types.StringType.get()).commit();
    }

    private static void writeAllTypesData(Table table) throws Exception {
        LOGGER.info("[WRITE] Writing all_types data from {}", ALL_ICEBERG_TYPES_PATH);
        String json = Files.readString(Paths.get(ALL_ICEBERG_TYPES_PATH));
        JsonNode row = OBJECT_MAPPER.readTree(json);
        GenericRecord template = GenericRecord.create(table.schema());
        List<Record> records = new ArrayList<>();
        Record record = template.copy();
        record.setField("bool_field", row.get("bool_field").asBoolean());
        record.setField("byte_field", row.get("byte_field").asInt());
        record.setField("short_field", row.get("short_field").asInt());
        record.setField("int_field", row.get("int_field").asInt());
        record.setField("long_field", row.get("long_field").asLong());
        record.setField("float_field", (float) row.get("float_field").asDouble());
        record.setField("double_field", row.get("double_field").asDouble());
        record.setField("decimal_field", new BigDecimal(row.get("decimal_field").asText()));
        record.setField("string_field", row.get("string_field").asText());
        record.setField("varchar_field", row.get("varchar_field").asText());
        record.setField("char_field", row.get("char_field").asText());
        record.setField("uuid_field", UUID.fromString(row.get("uuid_field").asText()));
        record.setField("binary_field", ByteBuffer.wrap(Base64.getDecoder().decode(row.get("binary_field").asText())));
        record.setField("fixed_field", Base64.getDecoder().decode(row.get("fixed_field").asText()));
        record.setField("date_field", LocalDate.ofEpochDay(row.get("date_field").asInt()));
        record.setField("time_field", LocalTime.ofNanoOfDay(row.get("time_field").asLong() * 1000));
        record.setField("timestamp_field", OffsetDateTime
                .ofInstant(java.time.Instant.ofEpochMilli(row.get("timestamp_field").asLong() / 1000), ZoneOffset.UTC));
        record.setField("timestamp_ntz_field", java.time.LocalDateTime.parse(row.get("timestamp_ntz_field").asText(),
                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        record.setField("timestamp_nano_field", OffsetDateTime.ofInstant(
                java.time.Instant.ofEpochSecond(0, row.get("timestamp_nano_field").asLong()), ZoneOffset.UTC));
        record.setField("interval_ym_field", row.get("interval_ym_field").asInt());
        record.setField("interval_dt_field", row.get("interval_dt_field").asLong());
        record.setField("geometry_field",
                ByteBuffer.wrap(Base64.getDecoder().decode(row.get("geometry_field").asText())));
        record.setField("geography_field",
                ByteBuffer.wrap(Base64.getDecoder().decode(row.get("geography_field").asText())));

        GenericRecord struct =
                GenericRecord.create(((Types.StructType) table.schema().findField("struct_field").type()));
        struct.setField("name", row.get("struct_field").get("name").asText());
        struct.setField("age", row.get("struct_field").get("age").asInt());
        struct.setField("active", row.get("struct_field").get("active").asBoolean());
        record.setField("struct_field", struct);

        record.setField("list_field", Arrays.asList(row.get("list_field").get(0).asText(),
                row.get("list_field").get(1).asText(), row.get("list_field").get(2).asText()));
        Map<String, String> mapValues = new HashMap<>();
        mapValues.put("key1", row.get("map_field").get("key1").asText());
        mapValues.put("key2", row.get("map_field").get("key2").asText());
        record.setField("map_field", mapValues);
        record.setField("variant_field", row.get("variant_field").asText());
        record.setField("unknown_field", row.get("unknown_field").isNull() ? null : row.get("unknown_field").asText());
        records.add(record);
        String path = table.location() + "/data/all_types.parquet";
        OutputFile out = table.io().newOutputFile(path);
        try (FileAppender<Record> writer =
                Parquet.write(out).schema(table.schema()).createWriterFunc(GenericParquetWriter::create).build()) {
            for (Record r : records) {
                writer.add(r);
            }
        }
        long bytes = out.toInputFile().getLength();
        LOGGER.info("[APPEND] file={} sizeBytes={} rows={}", path, bytes, records.size());
        DataFile df = DataFiles.builder(table.spec()).withPath(path).withRecordCount(records.size())
                .withFileSizeInBytes(bytes).withFormat(FileFormat.PARQUET).build();
        table.newAppend().appendFile(df).commit();
        LOGGER.info("[WRITE] all_types table committed with {} record(s)", records.size());
    }

    private static void printSnapshotSummary(Table table) {
        LOGGER.info("[SNAPSHOT SUMMARY]");
        Snapshot snap = table.currentSnapshot();
        LOGGER.info("currentSnapshotId={}", snap.snapshotId());
        LOGGER.info("schema={}", table.schema());
        LOGGER.info("spec={}", table.spec());
        LOGGER.info("plannedDataFiles={}", countFiles(table));
    }

    private static int countFiles(Table table) {
        int count = 0;
        for (FileScanTask ignored : table.newScan().planFiles()) {
            count++;
        }
        return count;
    }

    private static void validateCountsWithIceberg(Table table) throws Exception {
        long count = 0;
        try (CloseableIterable<Record> it = IcebergGenerics.read(table).build()) {
            for (Record r : it) {
                count++;
            }
        }
        LOGGER.info("[VALIDATION] IcebergGenerics.read(table) rowCount={} (expected 997)", count);
    }

    private static void ensureNamespace(org.apache.iceberg.nessie.NessieCatalog catalog, Namespace namespace) {
        String[] levels = namespace.levels();
        for (int i = 1; i <= levels.length; i++) {
            Namespace current = Namespace.of(Arrays.copyOf(levels, i));
            if (!catalog.namespaceExists(current)) {
                catalog.createNamespace(current);
                LOGGER.info("[INIT] Namespace created: {}", current);
            } else {
                LOGGER.info("[INIT] Namespace already exists: {}", current);
            }
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
        if (nessie != null) {
            nessie.stop();
            nessie = null;
        }
        LocalCloudUtilAdobeMock.shutdownSilently();
    }
}
