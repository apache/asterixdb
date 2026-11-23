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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.util.aws.AwsConstants;
import org.apache.asterix.external.awsclient.EnsureCloseAWSClientFactory;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.asterix.external.util.iceberg.nessie.NessieUtils;
import org.apache.asterix.test.external_dataset.deltalake.DeltaTableGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public final class IcebergSeeder {
    private static final String WAREHOUSE = "s3://iceberg-container/nessie/warehouse";
    private static final Namespace NAMESPACE = Namespace.of("cloud_primer");
    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "orders");
    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "customer", Types.StringType.get()),
                    Types.NestedField.required(3, "region", Types.StringType.get()),
                    Types.NestedField.required(4, "amount", Types.DoubleType.get()));

    private IcebergSeeder() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: IcebergSeeder <snapshot-properties-output-file>");
            System.exit(2);
        }
        Path propertiesFile = Path.of(args[0]);
        try (NessieCatalog catalog = createCatalog()) {
            if (!catalog.namespaceExists(NAMESPACE)) {
                catalog.createNamespace(NAMESPACE);
            }
            if (catalog.tableExists(TABLE_ID)) {
                catalog.dropTable(TABLE_ID, true);
            }

            Table table = catalog.buildTable(TABLE_ID, SCHEMA)
                    .withPartitionSpec(PartitionSpec.unpartitioned())
                    .withProperty(TableProperties.FORMAT_VERSION, "3")
                    .create();

            append(table, "snapshot-1.parquet", List.of(
                    row(1, "Ada", "west", 125.50),
                    row(2, "Linus", "north", 74.25),
                    row(3, "Grace", "west", 210.00)));
            long snapshot1 = table.currentSnapshot().snapshotId();
            long snapshot1Timestamp = table.currentSnapshot().timestampMillis();

            append(table, "snapshot-2.parquet", List.of(
                    row(4, "Edsger", "east", 95.00),
                    row(5, "Barbara", "north", 180.00),
                    row(6, "Donald", "east", 88.50)));
            long snapshot2 = table.currentSnapshot().snapshotId();

            Files.writeString(propertiesFile,
                    "SNAPSHOT_1=" + snapshot1 + System.lineSeparator()
                            + "SNAPSHOT_1_TIMESTAMP=" + snapshot1Timestamp + System.lineSeparator()
                            + "SNAPSHOT_2=" + snapshot2 + System.lineSeparator());
            System.out.printf("Seeded %s; snapshots=%d,%d%n", TABLE_ID, snapshot1, snapshot2);
        }
        seedDeltaTable();
    }

    private static void seedDeltaTable() throws Exception {
        Configuration configuration = new Configuration();
        DeltaTableGenerator.cleanBinaryDirectory(new java.io.File("."),
                DeltaTableGenerator.DELTA_GEN_BASEDIR);
        DeltaTableGenerator.prepareFileSizeOne(configuration);
        Path tableRoot = Path.of(DeltaTableGenerator.DELTA_FILE_SIZE_ONE);

        try (S3Client client = S3Client.builder().endpointOverride(URI.create("http://localhost:8001"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummyAccessKey", "dummySecretKey")))
                .region(Region.US_WEST_2)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build();
                var paths = Files.walk(tableRoot)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                String relative = tableRoot.relativize(path).toString().replace(java.io.File.separatorChar, '/');
                client.putObject(request -> request.bucket("iceberg-container").key("delta/orders/" + relative),
                        RequestBody.fromFile(path));
            });
        }
        System.out.println("Seeded Delta table delta/orders");
    }

    private static NessieCatalog createCatalog() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, "NESSIE");
        properties.put(CatalogProperties.CATALOG_IMPL, NessieCatalog.class.getName());
        properties.put("uri", "http://localhost:19120/api/v2");
        properties.put("warehouse", WAREHOUSE);
        properties.put(CatalogProperties.FILE_IO_IMPL, IcebergConstants.Aws.S3_FILE_IO);
        properties.put(AwsProperties.CLIENT_FACTORY, EnsureCloseAWSClientFactory.class.getName());
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL
                + AwsConstants.SERVICE_END_POINT_FIELD_NAME, "http://localhost:8001");
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL
                + AwsConstants.REGION_FIELD_NAME, "us-west-2");
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL + "pathStyleAddressing", "true");
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL
                + AwsConstants.ACCESS_KEY_ID_FIELD_NAME, "dummyAccessKey");
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL
                + AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, "dummySecretKey");
        NessieUtils.setNessieCatalogProperties(properties);
        return (NessieCatalog) IcebergUtils.initializeCatalogOnly(properties);
    }

    private static Record row(int id, String customer, String region, double amount) {
        GenericRecord row = GenericRecord.create(SCHEMA);
        row.setField("id", id);
        row.setField("customer", customer);
        row.setField("region", region);
        row.setField("amount", amount);
        return row;
    }

    private static void append(Table table, String fileName, List<Record> rows) throws Exception {
        String path = table.location() + "/data/" + fileName;
        OutputFile output = table.io().newOutputFile(path);
        try (FileAppender<Record> writer =
                Parquet.write(output).schema(SCHEMA).createWriterFunc(GenericParquetWriter::create).build()) {
            writer.addAll(rows);
        }
        DataFile file = DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(output.toInputFile().getLength()).withRecordCount(rows.size()).build();
        table.newAppend().appendFile(file).commit();
    }
}
