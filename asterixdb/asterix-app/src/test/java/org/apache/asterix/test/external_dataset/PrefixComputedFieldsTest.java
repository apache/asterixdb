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

import static org.apache.asterix.om.types.BuiltinType.AINT32;
import static org.apache.asterix.om.types.BuiltinType.ASTRING;

import java.util.Collections;
import java.util.List;

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

        String prefix1 = "";
        prefix = new ExternalDataPrefix(prefix1);
        assertEquals("", prefix.getOriginal());
        assertEquals("", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(Collections.emptyList(), prefix.getSegments());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldNames());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldTypes());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldSegmentIndexes());

        String prefix2 = "hotel";
        prefix = new ExternalDataPrefix(prefix2);
        assertEquals("hotel", prefix.getOriginal());
        assertEquals("hotel", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel"), prefix.getSegments());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldNames());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldTypes());
        assertEquals(Collections.emptyList(), prefix.getComputedFieldSegmentIndexes());

        String prefix3 = "hotel/{hotel-id:inT}/";
        prefix = new ExternalDataPrefix(prefix3);
        assertEquals("hotel/{hotel-id:inT}/", prefix.getOriginal());
        assertEquals("hotel/", prefix.getRoot());
        assertTrue(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "{hotel-id:inT}"), prefix.getSegments());
        assertEquals(List.of("hotel-id"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT32), prefix.getComputedFieldTypes());
        assertEquals(List.of(1), prefix.getComputedFieldSegmentIndexes());

        String prefix4 = "hotel/{hotel-id:int}-{hotel-name:sTRing}";
        prefix = new ExternalDataPrefix(prefix4);
        assertEquals("hotel/{hotel-id:int}-{hotel-name:sTRing}", prefix.getOriginal());
        assertEquals("hotel", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "{hotel-id:int}-{hotel-name:sTRing}"), prefix.getSegments());
        assertEquals(List.of("hotel-id", "hotel-name"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT32, ASTRING), prefix.getComputedFieldTypes());
        assertEquals(List.of(1, 1), prefix.getComputedFieldSegmentIndexes());

        String prefix5 = "hotel/something/{hotel-id:int}-{hotel-name:sTRing}/review/{year:int}-{month:int}-{day:int}/";
        prefix = new ExternalDataPrefix(prefix5);
        assertEquals("hotel/something/{hotel-id:int}-{hotel-name:sTRing}/review/{year:int}-{month:int}-{day:int}/",
                prefix.getOriginal());
        assertEquals("hotel/something/", prefix.getRoot());
        assertTrue(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "something", "{hotel-id:int}-{hotel-name:sTRing}", "review",
                "{year:int}-{month:int}-{day:int}"), prefix.getSegments());
        assertEquals(List.of("hotel-id", "hotel-name", "year", "month", "day"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT32, ASTRING, AINT32, AINT32, AINT32), prefix.getComputedFieldTypes());
        assertEquals(List.of(2, 2, 4, 4, 4), prefix.getComputedFieldSegmentIndexes());

        String prefix6 = "hotel/something/{hotel-id:int}-{hotel-name:sTRing}/review/{year:int}/{month:int}/{day:int}";
        prefix = new ExternalDataPrefix(prefix6);
        assertEquals("hotel/something/{hotel-id:int}-{hotel-name:sTRing}/review/{year:int}/{month:int}/{day:int}",
                prefix.getOriginal());
        assertEquals("hotel/something", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel", "something", "{hotel-id:int}-{hotel-name:sTRing}", "review", "{year:int}",
                "{month:int}", "{day:int}"), prefix.getSegments());
        assertEquals(List.of("hotel-id", "hotel-name", "year", "month", "day"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT32, ASTRING, AINT32, AINT32, AINT32), prefix.getComputedFieldTypes());
        assertEquals(List.of(2, 2, 4, 5, 6), prefix.getComputedFieldSegmentIndexes());

        String prefix7 = "hotel/{hotel.details.id:int}-{hotel-name:sTRing}";
        prefix = new ExternalDataPrefix(prefix7);
        assertEquals("hotel/{hotel.details.id:int}-{hotel-name:sTRing}", prefix.getOriginal());
        assertEquals("hotel", prefix.getRoot());
        assertFalse(prefix.isEndsWithSlash());
        assertEquals(List.of("hotel.details.id", "hotel-name"), prefix.getComputedFieldNames());
        assertEquals(List.of(AINT32, ASTRING), prefix.getComputedFieldTypes());
        assertEquals(List.of(1, 1), prefix.getComputedFieldSegmentIndexes());
    }
}
