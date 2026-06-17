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

import java.math.BigDecimal;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
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
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
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

    protected TestCaseContext tcCtx;

    public IcebergTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
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
