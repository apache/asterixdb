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

package org.apache.asterix.test.external_dataset;

import static org.apache.asterix.om.types.BuiltinType.AINT64;
import static org.apache.asterix.om.types.BuiltinType.ASTRING;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.external.util.ExternalDataPrefix;
import org.junit.Test;

import junit.framework.TestCase;

public class PrefixComputedFieldsTest extends TestCase {

    @Test
    public void test() throws Exception {
        ExternalDataPrefix prefix = new ExternalDataPrefix(Collections.emptyMap());
        assertEquals("", prefix.getOriginal());
        assertEquals("", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(Collections.emptyList(), prefix.getSegments());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldNames());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldTypes());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldSegmentIndexes());
        assertTrue(prefix.getIndexToComputedFieldsMap().isEmpty());

        String prefix1 = "";
        prefix = new ExternalDataPrefix(prefix1, null);
        assertEquals("", prefix.getOriginal());
        assertEquals("", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(Collections.emptyList(), prefix.getSegments());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldNames());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldTypes());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldSegmentIndexes());
        assertTrue(prefix.getIndexToComputedFieldsMap().isEmpty());

        String prefix2 = "hotel";
        prefix = new ExternalDataPrefix(prefix2, null);
        assertEquals("hotel", prefix.getOriginal());
        assertEquals("hotel", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel"), prefix.getSegments());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldNames());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldTypes());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldSegmentIndexes());
        assertTrue(prefix.getIndexToComputedFieldsMap().isEmpty());

        String prefix3 = "hotel/{hotel-id:inT}/";
        prefix = new ExternalDataPrefix(prefix3, null);
        assertEquals("hotel/{hotel-id:inT}/", prefix.getOriginal());
        assertEquals("hotel/", prefix.getRoot());
        assertTrue(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "{hotel-id:inT}"), prefix.getSegments());
        assertEquals(List.of("hotel-id"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT64), prefix.getComputedFieldTypes());
        assertEquals(List.of(1), prefix.getComputedFieldSegmentIndexes());
        assertEquals("(.+)", prefix.getIndexToComputedFieldsMap().get(1).getExpression());

        String prefix4 = "hotel/{hotel-id:int}-{hotel-name:sTRing}";
        prefix = new ExternalDataPrefix(prefix4, null);
        assertEquals("hotel/{hotel-id:int}-{hotel-name:sTRing}", prefix.getOriginal());
        assertEquals("hotel", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "{hotel-id:int}-{hotel-name:sTRing}"), prefix.getSegments());
        assertEquals(List.of("hotel-id", "hotel-name"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT64, ASTRING), prefix.getComputedFieldTypes());
        assertEquals(List.of(1, 1), prefix.getComputedFieldSegmentIndexes());
        assertEquals("(.+)-(.+)", prefix.getIndexToComputedFieldsMap().get(1).getExpression());

        String prefix5 = "hotel/something/{hotel-id:int}-{hotel-name:sTRing}/review/{year:int}-{month:int}-{day:int}/";
        prefix = new ExternalDataPrefix(prefix5, null);
        assertEquals("hotel/something/{hotel-id:int}-{hotel-name:sTRing}/review/{year:int}-{month:int}-{day:int}/",
                prefix.getOriginal());
        assertEquals("hotel/something/", prefix.getRoot());
        assertTrue(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "something", "{hotel-id:int}-{hotel-name:sTRing}", "review",
                "{year:int}-{month:int}-{day:int}"), prefix.getSegments());
        assertEquals(List.of("hotel-id", "hotel-name", "year", "month", "day"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT64, ASTRING, AINT64, AINT64, AINT64), prefix.getComputedFieldTypes());
        assertEquals(List.of(2, 2, 4, 4, 4), prefix.getComputedFieldSegmentIndexes());
        assertEquals("(.+)-(.+)", prefix.getIndexToComputedFieldsMap().get(2).getExpression());
        assertEquals("(.+)-(.+)-(.+)", prefix.getIndexToComputedFieldsMap().get(4).getExpression());

        String prefix6 = "hotel/something/{hotel-id:int}-{hotel-name:sTRing}/review/{year:int}/{month:int}/{day:int}";
        prefix = new ExternalDataPrefix(prefix6, null);
        assertEquals("hotel/something/{hotel-id:int}-{hotel-name:sTRing}/review/{year:int}/{month:int}/{day:int}",
                prefix.getOriginal());
        assertEquals("hotel/something", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "something", "{hotel-id:int}-{hotel-name:sTRing}", "review", "{year:int}",
                "{month:int}", "{day:int}"), prefix.getSegments());
        assertEquals(List.of("hotel-id", "hotel-name", "year", "month", "day"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT64, ASTRING, AINT64, AINT64, AINT64), prefix.getComputedFieldTypes());
        assertEquals(List.of(2, 2, 4, 5, 6), prefix.getComputedFieldSegmentIndexes());
        assertEquals("(.+)-(.+)", prefix.getIndexToComputedFieldsMap().get(2).getExpression());
        assertEquals("(.+)", prefix.getIndexToComputedFieldsMap().get(4).getExpression());
        assertEquals("(.+)", prefix.getIndexToComputedFieldsMap().get(5).getExpression());
        assertEquals("(.+)", prefix.getIndexToComputedFieldsMap().get(6).getExpression());

        String prefix7 = "hotel/{hotel.details.id:int}-{hotel-name:sTRing}";
        prefix = new ExternalDataPrefix(prefix7, null);
        assertEquals("hotel/{hotel.details.id:int}-{hotel-name:sTRing}", prefix.getOriginal());
        assertEquals("hotel", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel.details.id", "hotel-name"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT64, ASTRING), prefix.getComputedFieldTypes());
        assertEquals(List.of(1, 1), prefix.getComputedFieldSegmentIndexes());
        assertEquals("(.+)-(.+)", prefix.getIndexToComputedFieldsMap().get(1).getExpression());

        String prefix8 =
                "hotel/hotel-{hotel-id:int}-hotel-{hotel-name:sTRing}/review/year-{year:int}/{month:int}-month/day-{day:int}-day";
        prefix = new ExternalDataPrefix(prefix8, null);
        assertEquals(
                "hotel/hotel-{hotel-id:int}-hotel-{hotel-name:sTRing}/review/year-{year:int}/{month:int}-month/day-{day:int}-day",
                prefix.getOriginal());
        assertEquals("hotel", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "hotel-{hotel-id:int}-hotel-{hotel-name:sTRing}", "review", "year-{year:int}",
                "{month:int}-month", "day-{day:int}-day"), prefix.getSegments());
        assertEquals(List.of("hotel-id", "hotel-name", "year", "month", "day"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT64, ASTRING, AINT64, AINT64, AINT64), prefix.getComputedFieldTypes());
        assertEquals(List.of(1, 1, 3, 4, 5), prefix.getComputedFieldSegmentIndexes());
        assertEquals("hotel-(.+)-hotel-(.+)", prefix.getIndexToComputedFieldsMap().get(1).getExpression());
        assertEquals("year-(.+)", prefix.getIndexToComputedFieldsMap().get(3).getExpression());
        assertEquals("(.+)-month", prefix.getIndexToComputedFieldsMap().get(4).getExpression());
        assertEquals("day-(.+)-day", prefix.getIndexToComputedFieldsMap().get(5).getExpression());

        List<String> keys = new ArrayList<>();
        keys.add("hotel/hotel-1-hotel-name1/review/year-2000/January-month/day-1-day");
        keys.add("hotel/hotel-2-hotel-name2/review/year-2001/February-month/day-2-day");
        keys.add("hotel/hotel-3-hotel-name3/review/year-2002/March-month/day-3-day");

        for (String key : keys) {
            List<String> keySegments = ExternalDataPrefix.extractPrefixSegments(key);
            for (Map.Entry<Integer, ExternalDataPrefix.PrefixSegment> entry : prefix.getIndexToComputedFieldsMap()
                    .entrySet()) {
                int index = entry.getKey();
                ExternalDataPrefix.PrefixSegment segment = entry.getValue();

                String expression = segment.getExpression();

                String keySegment = keySegments.get(index);
                Matcher matcher = Pattern.compile(expression).matcher(keySegment);

                matcher.find();
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    System.out.println(matcher.group(i));
                }
            }
            System.out.println("\n");
        }
    }
}
