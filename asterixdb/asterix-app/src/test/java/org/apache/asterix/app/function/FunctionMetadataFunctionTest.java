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
package org.apache.asterix.app.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.junit.Test;

public class FunctionMetadataFunctionTest {

    @Test
    public void buildRecordBuiltinWithAlias() {
        String row = FunctionMetadataFunction.buildRecord("string_length", 2, "scalar", false,
                FunctionMetadataFunction.KIND_BUILTIN, null, Collections.singletonList("length"));
        assertEquals("{\"name\":\"string_length\",\"arity\":2,\"category\":\"scalar\",\"private\":false,"
                + "\"kind\":\"builtin\",\"dataverse\":null,\"aliases\":[\"length\"]}", row);
    }

    @Test
    public void buildRecordUdfWithDataverseAndNoAlias() {
        String row = FunctionMetadataFunction.buildRecord("fn_meta_udf", 1, "scalar", false,
                FunctionMetadataFunction.KIND_UDF, "Default", Collections.emptyList());
        assertEquals("{\"name\":\"fn_meta_udf\",\"arity\":1,\"category\":\"scalar\",\"private\":false,"
                + "\"kind\":\"udf\",\"dataverse\":\"Default\",\"aliases\":[]}", row);
    }

    @Test
    public void buildRecordRendersMultipleAliasesAndVarargs() {
        String row = FunctionMetadataFunction.buildRecord("array_concat", -1, "scalar", true,
                FunctionMetadataFunction.KIND_BUILTIN, null, Arrays.asList("a", "b"));
        assertEquals("{\"name\":\"array_concat\",\"arity\":-1,\"category\":\"scalar\",\"private\":true,"
                + "\"kind\":\"builtin\",\"dataverse\":null,\"aliases\":[\"a\",\"b\"]}", row);
    }

    @Test
    public void buildRecordEscapesQuotesBackslashesAndControlChars() {
        String name = "a\"b\\c" + (char) 1;
        String row = FunctionMetadataFunction.buildRecord(name, 0, "scalar", false,
                FunctionMetadataFunction.KIND_BUILTIN, null, Collections.emptyList());
        assertTrue(row.contains("\"name\":\"a\\\"b\\\\c\\u0001\""));
    }

    @Test
    public void categoryDistinguishesAllBuiltinKinds() {
        assertEquals("window", FunctionMetadataFunction.category(BuiltinFunctions.RANK));
        assertEquals("aggregate", FunctionMetadataFunction.category(BuiltinFunctions.COUNT));
        assertEquals("aggregate-scalar", FunctionMetadataFunction.category(BuiltinFunctions.SCALAR_COUNT));
        assertEquals("unnest", FunctionMetadataFunction.category(BuiltinFunctions.SCAN_COLLECTION));
        assertEquals("scalar", FunctionMetadataFunction.category(BuiltinFunctions.STRING_LENGTH));
    }

    @Test
    public void underscoreNormalizesHyphenatedNames() {
        assertEquals("a_b_c", FunctionMetadataFunction.underscore("a-b-c"));
        assertEquals("plain", FunctionMetadataFunction.underscore("plain"));
    }

    @Test
    public void aliasIndexIsSortedAndReportsCallableSpellings() {
        Map<String, List<String>> index = FunctionMetadataFunction.buildAliasIndex();
        assertEquals(Collections.singletonList("length"), index.get("string-length"));
        assertEquals(Arrays.asList("pos", "pos0", "position0"), index.get("position"));
        // Hyphenated alias keys must be reported verbatim: e.g. record-merge is callable only as the
        // delimited identifier `record-merge`; the underscore form record_merge does not resolve to
        // the mapping (the resolver checks getFunctionMapping before the underscore-to-hyphen rewrite).
        assertEquals(Collections.singletonList("record-merge"), index.get("object-merge"));
        assertEquals(Collections.singletonList("record-concat"), index.get("object-concat"));
        assertEquals(Collections.singletonList("record-get-fields"), index.get("object-get-fields"));
        assertEquals(Collections.singletonList("record-get-field-value"), index.get("object-get-field-value"));
        assertEquals(Collections.singletonList("record-add-fields"), index.get("object-add-fields"));
        assertEquals(Collections.singletonList("record-remove-fields"), index.get("object-remove-fields"));
        assertEquals(Collections.singletonList("array_agg-distinct"), index.get("arrayagg-distinct"));
    }
}
