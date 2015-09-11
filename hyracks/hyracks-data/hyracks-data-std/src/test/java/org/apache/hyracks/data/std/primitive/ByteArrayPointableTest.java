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

import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assert.*;

public class ByteArrayPointableTest {

    public static byte[] generatePointableBytes(byte[] bytes){
        byte[] ret = new byte[bytes.length + ByteArrayPointable.SIZE_OF_LENGTH];
        for (int i = 0; i < bytes.length; ++i){
            ret[i+ ByteArrayPointable.SIZE_OF_LENGTH] = bytes[i];
        }
        ByteArrayPointable.putLength(bytes.length, ret, 0);
        return ret;
    }

    @Test
    public void testCompareTo() throws Exception {
        byte [] bytes = generatePointableBytes(new byte[] { 1, 2, 3, 4});
        ByteArrayPointable byteArrayPointable = new ByteArrayPointable();
        byteArrayPointable.set(bytes, 0, bytes.length);

        testEqual(byteArrayPointable, generatePointableBytes(new byte[] { 1,2 ,3,4}));

        testLessThan(byteArrayPointable, generatePointableBytes(new byte[] {2}));
        testLessThan(byteArrayPointable, generatePointableBytes(new byte[] {1,2,3,5}));
        testLessThan(byteArrayPointable, generatePointableBytes(new byte[] {1,2,3,4,5}));

        testGreaterThan(byteArrayPointable, generatePointableBytes(new byte[] { }));
        testGreaterThan(byteArrayPointable, generatePointableBytes(new byte[] { 0}));
        testGreaterThan(byteArrayPointable, generatePointableBytes(new byte[] { 1,2,3}));

    }


    void testEqual(ByteArrayPointable pointable, byte [] bytes){
        assertTrue(pointable.compareTo(bytes, 0, bytes.length) == 0);
    }

    void testLessThan(ByteArrayPointable pointable, byte[] bytes){
        assertTrue(pointable.compareTo(bytes, 0, bytes.length) < 0);
    }

    void testGreaterThan(ByteArrayPointable pointable, byte[] bytes){
        assertTrue(pointable.compareTo(bytes, 0, bytes.length) > 0);
    }
}