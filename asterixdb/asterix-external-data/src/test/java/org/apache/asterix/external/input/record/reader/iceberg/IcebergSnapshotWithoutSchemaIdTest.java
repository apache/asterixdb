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
package org.apache.asterix.external.input.record.reader.iceberg;

import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SCHEMA_ID_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_ID_PROPERTY_KEY;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The table spec makes a snapshot's {@code schema-id} optional in every format version, so a snapshot may carry
 * none, and {@code Snapshot.schemaId()} is then {@code null}. Iceberg plans such a scan with the table's current
 * schema; the compiler and the reader must resolve the same schema rather than fail.
 * <p>
 * The reading itself is covered by the {@code no-schema-id} runtime suite. These cases are not reachable from a query:
 * the table evolving between compiling a query and running it, and a schema id that the table does not have.
 * <p>
 * The table evolves once, so the snapshot's own schema and the current one differ: the first snapshot was written with
 * {@code {id}}, the second after {@code name} was added. That is what tells "used the snapshot's schema" apart from
 * "used the current schema" in every assertion below.
 */
public class IcebergSnapshotWithoutSchemaIdTest {

    private File dir;
    private Table evolved;

    @Before
    public void createEvolvedTable() throws Exception {
        dir = Files.createTempDirectory("no-schema-id").toFile();
        String location = new File(dir, "t").getAbsolutePath();
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        evolved = new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema,
                PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "1"), location);
        append(evolved, "a");
        evolved.updateSchema().addColumn("name", Types.StringType.get()).commit();
        append(evolved, "b");
        Assert.assertEquals("precondition: two snapshots", 2, snapshots(evolved).size());
        Assert.assertNotEquals("precondition: the schema evolved between them", snapshots(evolved).get(0).schemaId(),
                snapshots(evolved).get(1).schemaId());
    }

    @Test
    public void readerKeepsThePinnedSchemaWhenAColumnIsAddedBeforeTheRun() throws Exception {
        assertReaderKeepsThePinnedSchema(
                () -> evolved.updateSchema().addColumn("later", Types.LongType.get()).commit());
    }

    @Test
    public void readerKeepsThePinnedSchemaWhenAProjectedColumnIsDroppedBeforeTheRun() throws Exception {
        assertReaderKeepsThePinnedSchema(() -> evolved.updateSchema().deleteColumn("name").commit());
    }

    /**
     * The compiler pins a snapshot without a schema id against the table as it is then; the table then evolves before
     * the query runs, without a new snapshot. The reader must still read with the pinned schema, not re-resolve the
     * table's current one.
     */
    private void assertReaderKeepsThePinnedSchema(Runnable evolutionBeforeTheRun) throws Exception {
        Table atCompile = rewritten(snapshot -> snapshot.remove("schema-id"));
        Map<String, String> configuration = pinned(snapshots(atCompile).get(0).snapshotId());
        new IcebergParquetRecordReaderFactory().setAndPinScanSnapshot(configuration, atCompile, atCompile.newScan());
        int pinnedSchemaId = Integer.parseInt(configuration.get(ICEBERG_SCHEMA_ID_PROPERTY_KEY));

        evolved.refresh();
        evolutionBeforeTheRun.run();
        evolved.refresh();
        Table atRun = rewritten(snapshot -> snapshot.remove("schema-id"));
        Assert.assertNotEquals("precondition: the table's current schema moved on", pinnedSchemaId,
                atRun.schema().schemaId());

        Schema resolved = IcebergFileRecordReader.schemaAtSnapshot(atRun, configuration);

        Assert.assertEquals("the schema the query was compiled with", pinnedSchemaId, resolved.schemaId());
        Assert.assertTrue(resolved.sameSchema(atCompile.schemas().get(pinnedSchemaId)));
    }

    @Test
    public void readerReportsASchemaIdTheTableDoesNotHave() throws Exception {
        Table table = rewritten(snapshot -> snapshot.put("schema-id", 99));
        long first = snapshots(table).get(0).snapshotId();
        assertFailsWith(ErrorCode.EXTERNAL_SOURCE_ERROR,
                () -> IcebergFileRecordReader.schemaAtSnapshot(table, pinned(first)));
    }

    @Test
    public void readerReportsAPinnedSchemaIdTheTableDoesNotHave() throws Exception {
        Table table = rewritten(snapshot -> snapshot.remove("schema-id"));
        Map<String, String> configuration = pinned(snapshots(table).get(0).snapshotId());
        configuration.put(ICEBERG_SCHEMA_ID_PROPERTY_KEY, "99");
        assertFailsWith(ErrorCode.EXTERNAL_SOURCE_ERROR,
                () -> IcebergFileRecordReader.schemaAtSnapshot(table, configuration));
    }

    /** A pinned configuration naming {@code snapshotId} and no schema id. */
    private static Map<String, String> pinned(long snapshotId) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ICEBERG_SNAPSHOT_ID_PROPERTY_KEY, String.valueOf(snapshotId));
        return configuration;
    }

    private interface Resolution {
        void run() throws Exception;
    }

    private static void assertFailsWith(ErrorCode expected, Resolution resolution) {
        try {
            resolution.run();
            Assert.fail("must fail with " + expected);
        } catch (CompilationException e) {
            Assert.assertEquals(expected.intValue(), e.getErrorCode());
        } catch (Exception e) {
            throw new AssertionError("must fail with " + expected + ", not " + e, e);
        }
    }

    private static void append(Table table, String name) {
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(table.location() + "/data/" + name)
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(10).withRecordCount(1).build()).commit();
    }

    private static List<Snapshot> snapshots(Table table) {
        List<Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        return snapshots;
    }

    /** The evolved table as a reader would load it from a metadata file whose snapshots were edited by {@code edit}. */
    private Table rewritten(Consumer<ObjectNode> edit) throws Exception {
        String metadataLocation = ((BaseTable) evolved).operations().current().metadataFileLocation();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode metadata = (ObjectNode) mapper.readTree(new File(metadataLocation));
        metadata.withArray("snapshots").forEach(snapshot -> edit.accept((ObjectNode) snapshot));
        File rewritten = new File(dir, "rewritten-" + System.nanoTime() + ".metadata.json");
        Files.writeString(rewritten.toPath(), mapper.writeValueAsString(metadata), StandardCharsets.UTF_8);
        HadoopFileIO io = new HadoopFileIO(new org.apache.hadoop.conf.Configuration());
        return new BaseTable(new StaticTableOperations(rewritten.getAbsolutePath(), io), "rewritten");
    }
}
