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
package org.apache.asterix.external.parser.test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;

public class ParserContextPoolTest {

    @Test
    public void eachNestingLevelGetsItsOwnInstanceBack() {
        ParserContext context = new ParserContext();
        int depth = 4;
        IMutableValueStorage[][] buffers = new IMutableValueStorage[2][depth];
        IARecordBuilder[][] builders = new IARecordBuilder[2][depth];
        for (int doc = 0; doc < 2; doc++) {
            for (int level = 0; level < depth; level++) {
                buffers[doc][level] = context.enterObject();
                builders[doc][level] = context.getObjectBuilder(RecordUtil.FULLY_OPEN_RECORD_TYPE);
            }
            for (int level = depth - 1; level >= 0; level--) {
                context.exitObject(buffers[doc][level], null, builders[doc][level]);
            }
        }
        for (int level = 0; level < depth; level++) {
            Assert.assertSame("temp buffer at level " + level, buffers[0][level], buffers[1][level]);
            Assert.assertSame("record builder at level " + level, builders[0][level], builders[1][level]);
        }
    }

    @Test
    public void levelsBelowALargeValueKeepSmallBuffers() throws Exception {
        // root -> a -> b -> c[...]: the array's content is copied into the temp buffers of root, a and b, while the
        // array's own temp buffer only ever holds one item
        String items = String.join(",", Collections.nCopies(3000, '"' + "x".repeat(100) + '"'));
        byte[] doc = ("{\"a\":{\"b\":{\"c\":[" + items + "]}}}").getBytes(StandardCharsets.UTF_8);
        int largeContent = 300_000;
        Assert.assertTrue(doc.length > largeContent);

        InspectableParser parser = new InspectableParser();
        parser.parse(doc);
        parser.parse(doc);

        int large = 0;
        for (int level = 0; level < 4; level++) {
            if (parser.context().enterObject().getByteArray().length >= largeContent) {
                large++;
            }
        }
        Assert.assertEquals("temp buffers pooled at the size of the array", 3, large);
    }

    @Test
    public void reusedParserProducesTheSameRecords() throws Exception {
        byte[] large = ("{\"a\":{\"b\":[{\"s\":\"" + "x".repeat(2 * 1024 * 1024) + "\"},{\"n\":1}]},\"z\":2}")
                .getBytes(StandardCharsets.UTF_8);
        byte[] small = "{\"a\":{\"b\":[{\"n\":3},[4,{\"m\":5}]]},\"z\":[6]}".getBytes(StandardCharsets.UTF_8);
        InspectableParser reused = new InspectableParser();
        for (byte[] doc : new byte[][] { large, small, large, small, small }) {
            Assert.assertArrayEquals(new InspectableParser().parse(doc), reused.parse(doc));
        }
    }

    @Test
    public void pooledBytesCoverEveryCopyOfALargeValue() throws Exception {
        String items = String.join(",", Collections.nCopies(3000, '"' + "x".repeat(100) + '"'));
        byte[] doc = ("{\"report\":{\"items\":[" + items + "]}}").getBytes(StandardCharsets.UTF_8);
        InspectableParser parser = new InspectableParser();
        Assert.assertEquals(0, parser.context().getPooledBytes());
        parser.parse(doc);
        // the items list builder, and a temp buffer and a record builder for each of report and the root
        long pooled = parser.context().getPooledBytes();
        Assert.assertTrue(pooled + " < 5 x " + doc.length, pooled >= 5L * (doc.length - 32));
        Assert.assertEquals(pooled, parser.getPooledBytes());
    }

    @Test
    public void pooledBytesHoldWhileAValueIsBeingParsed() throws Exception {
        InspectableParser parser = new InspectableParser();
        parser.parse(("{\"a\":{\"b\":\"" + "x".repeat(100_000) + "\"}}").getBytes(StandardCharsets.UTF_8));
        long pooled = parser.getPooledBytes();
        ParserContext context = parser.context();
        IMutableValueStorage buffer = context.enterObject();
        IARecordBuilder builder = context.getObjectBuilder(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        Assert.assertTrue(buffer.getByteArray().length >= 100_000);
        Assert.assertEquals(pooled, parser.getPooledBytes());
        context.exitObject(buffer, null, builder);
    }

    @Test
    public void failedParseLeavesThePoolsMeasured() throws Exception {
        byte[] small = "{\"a\":{\"b\":[1,{\"c\":2}]}}".getBytes(StandardCharsets.UTF_8);
        String value = "x".repeat(100_000);
        byte[] truncated = ("{\"a\":{\"b\":[\"" + value + "\",{\"c\":").getBytes(StandardCharsets.UTF_8);
        byte[] large = ("{\"a\":{\"b\":[\"" + value + "\",{\"c\":2}]}}").getBytes(StandardCharsets.UTF_8);
        InspectableParser parser = new InspectableParser();
        parser.parse(small);
        long afterSmall = parser.getPooledBytes();
        try {
            parser.parse(truncated);
            Assert.fail("parsed a truncated document");
        } catch (HyracksDataException e) {
            // expected
        }
        // the buffers that grew before the failure went back to the pools
        long afterFailure = parser.getPooledBytes();
        Assert.assertTrue(afterFailure + " < " + afterSmall + " + 100000", afterFailure >= afterSmall + 100_000);
        // and later documents are still measured
        Assert.assertArrayEquals(new InspectableParser().parse(large), parser.parse(large));
        Assert.assertTrue(parser.getPooledBytes() >= afterFailure + 100_000);
    }

    @Test
    public void closedRecordsAreParsedAndMeasured() throws Exception {
        ARecordType inner = new ARecordType("inner", new String[] { "n" }, new IAType[] { BuiltinType.AINT64 }, false);
        ARecordType type = new ARecordType("outer", new String[] { "id", "name", "in" },
                new IAType[] { BuiltinType.AINT64, BuiltinType.ASTRING, inner }, true);
        byte[] doc = "{\"id\":1,\"name\":\"n\",\"in\":{\"n\":2},\"extra\":[3]}".getBytes(StandardCharsets.UTF_8);
        byte[] missingField = "{\"id\":1,\"in\":{\"n\":2}}".getBytes(StandardCharsets.UTF_8);
        InspectableParser reused = new InspectableParser(type);
        byte[] expected = new InspectableParser(type).parse(doc);
        Assert.assertArrayEquals(expected, reused.parse(doc));
        try {
            reused.parse(missingField);
            Assert.fail("parsed a record without a required field");
        } catch (HyracksDataException e) {
            // expected
        }
        // the null bitmaps come back from the pool cleared
        Assert.assertArrayEquals(expected, reused.parse(doc));
        Assert.assertTrue(reused.getPooledBytes() > 0);
    }

    private static class InspectableParser extends JSONDataParser {
        InspectableParser() {
            this(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        }

        InspectableParser(ARecordType type) {
            super(type, new JsonFactory());
        }

        ParserContext context() {
            return parserContext;
        }

        byte[] parse(byte[] doc) throws Exception {
            ArrayBackedValueStorage out = new ArrayBackedValueStorage();
            setInputStream(new ByteArrayInputStream(doc));
            Assert.assertTrue(parse(out.getDataOutput()));
            return Arrays.copyOfRange(out.getByteArray(), out.getStartOffset(), out.getStartOffset() + out.getLength());
        }
    }
}
