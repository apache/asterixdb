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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.data.std.api.IValueReference;
import org.junit.Test;

/**
 * A projected path through a null object reads null, as a row does: {@code a.b.c} of {@code {"a": {"b": null}}} is
 * null, whatever columns the component has under {@code b}.
 */
public class NullAncestorProjectionTest {
    private static final ARecordType B_C =
            new ARecordType("b", new String[] { "c" }, new IAType[] { BuiltinType.ANY }, true);
    private static final ARecordType A_B_C = new ARecordType("a", new String[] { "b" }, new IAType[] { B_C }, true);
    private static final ARecordType ROOT =
            new ARecordType("ALL", new String[] { "id", "a" }, new IAType[] { BuiltinType.ANY, A_B_C }, true);

    @Test
    public void aNullObjectAloneInItsComponent() throws Exception {
        assertProjected(List.of("{\"id\": 15, \"a\": {\"b\": null}}"), "{\"id\":15,\"a\":{\"b\":null}}");
    }

    @Test
    public void aNullObjectBesideEmptyOnes() throws Exception {
        assertProjected(
                List.of("{\"id\": 2, \"a\": {}}", "{\"id\": 4, \"a\": {\"b\": {}}}",
                        "{\"id\": 15, \"a\": {\"b\": null}}"),
                "{\"id\":2,\"a\":{}}", "{\"id\":4,\"a\":{\"b\":{}}}", "{\"id\":15,\"a\":{\"b\":null}}");
    }

    @Test
    public void aNullObjectBesideOnesThatHaveTheField() throws Exception {
        assertProjected(
                List.of("{\"id\": 2, \"a\": {}}", "{\"id\": 4, \"a\": {\"b\": {}}}",
                        "{\"id\": 5, \"a\": {\"b\": {\"c\": null}}}", "{\"id\": 15, \"a\": {\"b\": null}}"),
                "{\"id\":2,\"a\":{}}", "{\"id\":4,\"a\":{\"b\":{}}}", "{\"id\":5,\"a\":{\"b\":{\"c\":null}}}",
                "{\"id\":15,\"a\":{\"b\":null}}");
    }

    private static void assertProjected(List<String> records, String... expected) throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component component = harness.writeComponent(records);
        List<String> actual = new ArrayList<>();
        for (IValueReference record : harness.assembleProjected(component, ROOT, null)) {
            actual.add(ColumnFlushHarness.json(record).replace(" ", ""));
        }
        assertEquals(List.of(expected), actual);
    }
}
