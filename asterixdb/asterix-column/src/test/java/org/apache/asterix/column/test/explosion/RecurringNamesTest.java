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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Under the default thresholds: a bounded record whose many optional fields are rare shows its names for the first time
 * one window after another, as a key set does, but its names come back and run out, so it stays columnar; a key set
 * whose names come back too, but never run out, still collapses.
 */
public class RecurringNamesTest {
    private static final int COMMON_FIELDS = 10;

    @Before
    public void configure() {
        CollapseSettings.install();
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void aRecordOfManyRareOptionalFieldsStaysColumnar() throws Exception {
        for (int optional : new int[] { 60, 100 }) {
            List<String> lines = new ArrayList<>();
            Random random = new Random(42);
            for (int d = 0; d < 3000; d++) {
                lines.add("{\"id\": " + d + sparseFields(random, optional, 0.01) + "}");
            }
            assertFalse(optional + " optional fields at 1%: the record must stay columnar",
                    WideValueCollapseTest.schemaOf(lines).getRoot().isCollapsed());
        }
    }

    @Test
    public void aNestedRecordOfManyRareOptionalFieldsStaysColumnar() throws Exception {
        List<String> lines = new ArrayList<>();
        Random random = new Random(42);
        for (int d = 0; d < 3000; d++) {
            lines.add("{\"id\": " + d + ", \"rec\": {\"x\": 0" + sparseFields(random, 100, 0.01) + "}}");
        }
        ObjectSchemaNode root = WideValueCollapseTest.schemaOf(lines).getRoot();
        ObjectSchemaNode rec = (ObjectSchemaNode) root.getChildren().get(1);
        assertFalse("100 optional fields at 1% under rec: rec must stay columnar", rec.isCollapsed());
    }

    /** The same in small flushes through one schema, as an index writes them: its fields come back between batches. */
    @Test
    public void rareOptionalFieldsInSmallFlushesStayColumnar() throws Exception {
        for (double density : new double[] { 0.003, 0.005 }) {
            ColumnFlushHarness harness = new ColumnFlushHarness();
            FlushColumnMetadata md = harness.freshMetadata(harness.createFile());
            Random random = new Random(42);
            for (int flush = 0, d = 0; flush < 20; flush++) {
                List<String> lines = new ArrayList<>();
                for (int i = 0; i < 100; i++, d++) {
                    StringBuilder rec = new StringBuilder("{\"x\": 1");
                    for (int o = 0; o < 100; o++) {
                        if (random.nextDouble() < density) {
                            rec.append(", \"o").append(o).append("\": ").append(o);
                        }
                    }
                    lines.add("{\"id\": " + d + ", \"rec\": " + rec.append('}') + "}");
                }
                harness.writeNextComponent(md, lines);
            }
            ObjectSchemaNode rec = (ObjectSchemaNode) md.getRoot().getChildren().get(1);
            assertFalse("100 optional fields at " + density + " in flushes of 100: rec must stay columnar",
                    rec.isCollapsed());
        }
    }

    /** Users who come back, and a new one every ten documents: never running out, the names collapse their node. */
    @Test
    public void aKeySetWhoseNamesComeBackStillCollapses() throws Exception {
        List<String> lines = new ArrayList<>();
        Random random = new Random(42);
        int users = 1;
        for (int d = 0; d < 4000; d++) {
            if (d % 10 == 0) {
                users++;
            }
            StringBuilder line = new StringBuilder("{\"id\": ").append(d).append(", \"users\": {");
            line.append("\"u").append(users - 1).append("\": ").append(d);
            for (int r = 0; r < 3; r++) {
                line.append(", \"u").append(random.nextInt(users - 1)).append("r").append(r).append("\": ").append(d);
            }
            lines.add(line.append("}}").toString());
        }
        ObjectSchemaNode root = WideValueCollapseTest.schemaOf(lines).getRoot();
        assertTrue("users must collapse", ((ObjectSchemaNode) root.getChildren().get(1)).isCollapsed());
    }

    private static String sparseFields(Random random, int fields, double density) {
        StringBuilder sb = new StringBuilder();
        for (int c = 0; c < COMMON_FIELDS; c++) {
            sb.append(", \"c").append(c).append("\": ").append(c);
        }
        for (int o = 0; o < fields; o++) {
            if (random.nextDouble() < density) {
                sb.append(", \"o").append(o).append("\": ").append(o);
            }
        }
        return sb.toString();
    }
}
