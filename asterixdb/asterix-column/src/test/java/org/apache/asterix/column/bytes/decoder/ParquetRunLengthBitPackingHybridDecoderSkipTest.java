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
package org.apache.asterix.column.bytes.decoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import org.apache.asterix.column.bytes.encoder.ParquetRunLengthBitPackingHybridEncoder;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link ParquetRunLengthBitPackingHybridDecoder#skipWhile(int, int)} must consume exactly the entries a sequence of
 * {@link ParquetRunLengthBitPackingHybridDecoder#readInt()} calls would have returned as equal to the value, and
 * leave the first differing entry in place, whichever way the encoder happened to lay the entries out.
 */
public class ParquetRunLengthBitPackingHybridDecoderSkipTest {
    private static final int BIT_WIDTH = 3;
    private static final int MAX_LEVEL = 3;
    private static final int DELIMITER = 1;

    @Test
    public void skipsWholeRunsOfArrayLevels() throws Exception {
        // the shape an array of 384 doubles per record produces: 384 max-level entries then a delimiter
        int records = 1000;
        int[] levels = new int[records * 385];
        for (int r = 0, i = 0; r < records; r++) {
            for (int j = 0; j < 384; j++) {
                levels[i++] = MAX_LEVEL;
            }
            levels[i++] = DELIMITER;
        }
        ParquetRunLengthBitPackingHybridDecoder decoder = decode(levels);
        for (int r = 0; r < records; r++) {
            Assert.assertEquals("record " + r, 384, decoder.skipWhile(MAX_LEVEL, Integer.MAX_VALUE));
            Assert.assertEquals("delimiter of record " + r, 0, decoder.skipWhile(MAX_LEVEL, Integer.MAX_VALUE));
            Assert.assertEquals("delimiter of record " + r, DELIMITER, decoder.readInt());
        }
    }

    @Test
    public void honoursTheLimitInsideARun() throws Exception {
        int[] levels = new int[500];
        java.util.Arrays.fill(levels, MAX_LEVEL);
        levels[499] = DELIMITER;
        ParquetRunLengthBitPackingHybridDecoder decoder = decode(levels);
        Assert.assertEquals(10, decoder.skipWhile(MAX_LEVEL, 10));
        Assert.assertEquals(MAX_LEVEL, decoder.readInt());
        Assert.assertEquals(488, decoder.skipWhile(MAX_LEVEL, 1000));
        Assert.assertEquals(DELIMITER, decoder.readInt());
    }

    @Test
    public void stopsAtTheFirstDifferentEntryInBitPackedGroups() throws Exception {
        // alternating values never form a run, so the encoder bit-packs everything
        int[] levels = new int[64];
        for (int i = 0; i < levels.length; i++) {
            levels[i] = i % 2 == 0 ? MAX_LEVEL : DELIMITER;
        }
        ParquetRunLengthBitPackingHybridDecoder decoder = decode(levels);
        for (int i = 0; i < levels.length; i += 2) {
            Assert.assertEquals(1, decoder.skipWhile(MAX_LEVEL, 100));
            Assert.assertEquals(0, decoder.skipWhile(MAX_LEVEL, 100));
            Assert.assertEquals(DELIMITER, decoder.readInt());
        }
    }

    @Test
    public void returnsZeroAtTheEnd() throws Exception {
        ParquetRunLengthBitPackingHybridDecoder decoder = decode(new int[] { MAX_LEVEL, MAX_LEVEL });
        Assert.assertEquals(2, decoder.skipWhile(MAX_LEVEL, 5));
        Assert.assertEquals(0, decoder.skipWhile(MAX_LEVEL, 5));
    }

    @Test
    public void interleavedSkipsAndReadsAgreeWithSequentialReads() throws Exception {
        Random random = new Random(20260920);
        for (int round = 0; round < 200; round++) {
            int[] levels = randomLevels(random);
            ParquetRunLengthBitPackingHybridDecoder decoder = decode(levels);
            int position = 0;
            while (position < levels.length) {
                int action = random.nextInt(3);
                if (action == 0) {
                    int n = random.nextInt(Math.min(50, levels.length - position) + 1);
                    decoder.skip(n);
                    position += n;
                } else if (action == 1) {
                    int value = random.nextInt(MAX_LEVEL + 1);
                    // bounded by the remaining entries, as readers bound it by their value count
                    int limit = Math.min(random.nextInt(50), levels.length - position);
                    int expected = 0;
                    while (expected < limit && position + expected < levels.length
                            && levels[position + expected] == value) {
                        expected++;
                    }
                    Assert.assertEquals("round " + round + " at " + position, expected,
                            decoder.skipWhile(value, limit));
                    position += expected;
                } else {
                    Assert.assertEquals("round " + round + " at " + position, levels[position++], decoder.readInt());
                }
            }
        }
    }

    private static int[] randomLevels(Random random) {
        // runs of random length, short enough to mix run-length runs with bit-packed groups
        int size = 1 + random.nextInt(3000);
        int[] levels = new int[size];
        int i = 0;
        while (i < size) {
            int value = random.nextInt(MAX_LEVEL + 1);
            int run = 1 + random.nextInt(random.nextBoolean() ? 3 : 60);
            for (int j = 0; j < run && i < size; j++) {
                levels[i++] = value;
            }
        }
        return levels;
    }

    private static ParquetRunLengthBitPackingHybridDecoder decode(int[] levels) throws Exception {
        ParquetRunLengthBitPackingHybridEncoder encoder = new ParquetRunLengthBitPackingHybridEncoder(BIT_WIDTH);
        for (int level : levels) {
            encoder.writeInt(level);
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        encoder.toBytes().writeAllTo(bos);
        ParquetRunLengthBitPackingHybridDecoder decoder = new ParquetRunLengthBitPackingHybridDecoder(BIT_WIDTH);
        decoder.reset(new ByteArrayInputStream(bos.toByteArray()));
        return decoder;
    }
}
