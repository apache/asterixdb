/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.std.util;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

public class MathTest {

    @Test
    public void testLog2() {
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 31; i++) {
            assertTrue(MathUtil.log2Floor((int) Math.pow(2, i)) == i);
            for(int x = 0; x < 10; x++){
                float extra = random.nextFloat();
                while (extra >= 1.0){
                    extra = random.nextFloat();
                }
                assertTrue(MathUtil.log2Floor((int) Math.pow(2, i + extra)) == i);
            }
        }
    }
}