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
package org.apache.asterix.column.test.explosion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.common.buffer.DummyPage;
import org.apache.asterix.column.common.test.TestCase;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.metadata.schema.CollapseDetectorSettings;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.test.bytes.AbstractBytesTest;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Round-trips documents whose field-name set is unbounded through the collapsed map representation: write them, read
 * them back, and require the reconstructed records to equal the originals.
 * <p>
 * Collapsing is a storage decision, so it has to be invisible above the storage layer — a document written as
 * {@code {id, uuid1, uuid2}} must read back with exactly those fields, not as a nested sub-object.
 * <p>
 * Runs with the detector thresholds lowered, because the defaults need far more documents than this fixture writes.
 * The test asserts the node actually collapsed, so a run with default thresholds fails loudly rather than passing
 * while exercising nothing.
 */
public class MapRoundTripTest extends AbstractBytesTest {
    private static final int DOCUMENTS = 48;
    private static final int KEYS_PER_DOCUMENT = 3;
    /** Set by {@code -Dasterix.column.map.test.nested=true} to put object values under the keys. */
    private static final boolean NESTED = Boolean.getBoolean("asterix.column.map.test.nested");

    private final List<String> inputLines;

    public MapRoundTripTest() throws Exception {
        this(generate());
    }

    private MapRoundTripTest(List<String> lines) throws Exception {
        super(writeFixture(lines));
        this.inputLines = lines;
    }

    private static List<String> generate() {
        List<String> lines = new ArrayList<>(DOCUMENTS);
        for (int d = 0; d < DOCUMENTS; d++) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"id\": ").append(d);
            sb.append(", \"name\": \"n").append(d).append("\"");
            for (int k = 0; k < KEYS_PER_DOCUMENT; k++) {
                sb.append(", \"").append(key(d, k)).append("\": ");
                if (NESTED) {
                    // Shape A: uniform object values under the collapsed keys. This is what every real corpus looks
                    // like, so the scalar-only case is not enough on its own.
                    sb.append("{\"a\": ").append(d * 10 + k).append(", \"b\": \"v").append(d).append("_").append(k)
                            .append("\"}");
                } else {
                    sb.append(d * 10 + k);
                }
            }
            sb.append("}");
            lines.add(sb.toString());
        }
        return lines;
    }

    private static String key(int doc, int index) {
        long h = doc * 0x9E3779B97F4A7C15L + index * 0xC2B2AE3D27D4EB4FL;
        return String.format("k%08x-%04x-%012x", (int) (h >>> 32), doc & 0xFFFF, Math.abs(h) % 0xFFFFFFFFFFFFL);
    }

    private static TestCase writeFixture(List<String> lines) throws IOException {
        File dir = Files.createTempDirectory("map-roundtrip").toFile();
        File data = new File(dir, "map-roundtrip.json");
        Files.write(data.toPath(), String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
        File out = new File(dir, "out");
        Files.createDirectory(out.toPath());
        return new TestCase(data, new File(dir, "map-roundtrip.result"), out);
    }

    @Test
    public void roundTrip() throws Exception {
        List<String> outputLines = writeThenRead(ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE);
        assertEquals("one reconstructed record per written document", DOCUMENTS, outputLines.size());
        for (int i = 0; i < DOCUMENTS; i++) {
            assertEquals("record " + i + " must survive the round trip", parse(inputLines.get(i)),
                    parse(outputLines.get(i)));
        }
    }

    /**
     * Projecting a single field is a different read path from reading whole records, and it is the one that breaks
     * silently: the clipper resolves requested names through the field-name dictionary, and a collapsed node's keys
     * are deliberately never interned. An unresolved name must fall back to the map, otherwise the query answers
     * "missing" for a field that is present.
     * <p>
     * Both sides of the collapse boundary are checked — a key from the first document (an ordinary column) and one
     * from the last (inside the map).
     */
    @Test
    public void projectedRead() throws Exception {
        assertProjects(0, 0);
        assertProjects(DOCUMENTS - 1, 0);
    }

    private void assertProjects(int doc, int keyIndex) throws Exception {
        String fieldName = key(doc, keyIndex);
        int scalar = doc * 10 + keyIndex;
        // Whitespace-stripped, matching how parse() normalises values.
        String expected =
                NESTED ? "{\"a\":" + scalar + ",\"b\":\"v" + doc + "_" + keyIndex + "\"}" : String.valueOf(scalar);
        ARecordType requested =
                ProjectionFiltrationTypeUtil.getRecordType(Collections.singletonList(List.of(fieldName)));
        List<String> outputLines = writeThenRead(requested);
        assertEquals("one reconstructed record per written document", DOCUMENTS, outputLines.size());

        List<String> withField = new ArrayList<>();
        for (String line : outputLines) {
            if (parse(line).containsKey(fieldName)) {
                withField.add(line);
            }
        }
        assertEquals(
                "exactly one document carries " + fieldName + ", so projecting it must yield exactly one value; "
                        + "an empty result means the name failed to resolve and the map was not consulted",
                1, withField.size());
        assertEquals("projected value for " + fieldName, expected, parse(withField.get(0)).get(fieldName));
        // Proves the requested names are pushed into the map rather than the whole object being rebuilt and filtered
        // afterwards: the document carries KEYS_PER_DOCUMENT collapsed entries, and only the requested one may
        // survive reconstruction.
        assertEquals(
                "a projected read must not reconstruct entries the query did not ask for, but got " + withField.get(0),
                1, parse(withField.get(0)).size());
    }

    /** Writes the fixture, then reads it back under the given requested type. */
    private List<String> writeThenRead(ARecordType requestedType) throws Exception {
        int fileId = createFile();
        FlushColumnMetadata columnMetadata = prepareNewFile(fileId);
        List<IValueReference> records = getParsedRecords();
        assertEquals(DOCUMENTS, records.size());

        List<DummyPage> pageZeros = transform(fileId, columnMetadata, records, records.size());

        assertTrue("the fixture must actually collapse; check the lowered thresholds",
                columnMetadata.getRoot().isMapified());
        int columns = columnMetadata.getNumberOfColumns();
        int shredded = DOCUMENTS * KEYS_PER_DOCUMENT;
        assertTrue("collapsing must bound the column count well below one per distinct key: " + columns + " vs "
                + shredded, columns < shredded);

        QueryColumnMetadata readMetadata = QueryColumnMetadata.create(columnMetadata.getDatasetType(),
                columnMetadata.getNumberOfPrimaryKeys(), columnMetadata.serializeColumnsMetadata(),
                new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE, requestedType, Collections.emptyMap(),
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY);
        writeResult(fileId, readMetadata, pageZeros);
        return Files.readAllLines(testCase.getOutputFile().toPath());
    }

    /**
     * Field order is not preserved through the columnar layout, so records are compared as name -> value maps. For
     * nested values the whole sub-object is captured as the value, with whitespace stripped so the input's spacing
     * and the printer's do not matter.
     */
    private static Map<String, String> parse(String json) {
        Map<String, String> fields = new LinkedHashMap<>();
        String body = json.trim();
        body = body.substring(1, body.length() - 1);
        int depth = 0;
        int start = 0;
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '{' || c == '[') {
                depth++;
            } else if (c == '}' || c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(body.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(body.substring(start));
        for (String part : parts) {
            int colon = part.indexOf(':');
            if (colon < 0) {
                continue;
            }
            String name = part.substring(0, colon).trim().replaceAll("^\"|\"$", "");
            fields.put(name, part.substring(colon + 1).replaceAll("\\s+", ""));
        }
        return fields;
    }

    @Override
    protected void writeResult(int fileId, QueryColumnMetadata readMetadata, List<DummyPage> pageZeroList)
            throws IOException {
        super.writeResult(fileId, readMetadata, pageZeroList);
    }

    @Override
    protected CollapseDetectorSettings collapseDetectorSettings() {
        return CollapseSettings.current();
    }

    @Before
    public void lowerThresholds() {
        CollapseSettings.install(CollapseSettings.lowered(8, 1));
    }

    @After
    public void resetThresholds() {
        CollapseSettings.reset();
    }
}
