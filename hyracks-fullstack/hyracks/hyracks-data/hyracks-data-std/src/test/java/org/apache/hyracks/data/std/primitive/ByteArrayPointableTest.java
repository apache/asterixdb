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

package org.apache.hyracks.data.std.primitive;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ByteArrayPointableTest {

    @Test
    public void testCompareTo() throws Exception {
        ByteArrayPointable byteArrayPointable =
                ByteArrayPointable.generatePointableFromPureBytes(new byte[] { 1, 2, 3, 4 });

        testEqual(byteArrayPointable, ByteArrayPointable.generatePointableFromPureBytes(new byte[] { 1, 2, 3, 4 }));

        testLessThan(byteArrayPointable, ByteArrayPointable.generatePointableFromPureBytes(new byte[] { 2 }, 0, 1));
        testLessThan(byteArrayPointable, ByteArrayPointable.generatePointableFromPureBytes(new byte[] { 1, 2, 3, 5 }));
        testLessThan(byteArrayPointable,
                ByteArrayPointable.generatePointableFromPureBytes(new byte[] { 1, 2, 3, 4, 5 }));

        testGreaterThan(byteArrayPointable, ByteArrayPointable.generatePointableFromPureBytes(new byte[] {}));
        testGreaterThan(byteArrayPointable, ByteArrayPointable.generatePointableFromPureBytes(new byte[] { 0 }));
        testGreaterThan(byteArrayPointable, ByteArrayPointable.generatePointableFromPureBytes(new byte[] { 1, 2, 3 }));

    }

    void testEqual(ByteArrayPointable pointable, ByteArrayPointable bytes) {
        assertTrue(pointable.compareTo(bytes) == 0);
    }

    void testLessThan(ByteArrayPointable pointable, ByteArrayPointable bytes) {
        assertTrue(pointable.compareTo(bytes) < 0);
    }

    void testGreaterThan(ByteArrayPointable pointable, ByteArrayPointable bytes) {
        assertTrue(pointable.compareTo(bytes) > 0);
    }
}
