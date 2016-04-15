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
package org.apache.hyracks.dataflow.std.parallel;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.std.parallel.histogram.terneray.SequentialAccessor;
import org.apache.hyracks.dataflow.std.parallel.histogram.terneray.TernaryMemoryTrie;
import org.apache.hyracks.dataflow.std.parallel.util.HistogramUtils;
import org.junit.Before;
import org.junit.Test;
import org.apache.hyracks.util.string.UTF8StringUtil;

import junit.framework.TestCase;

/**
 * @author michael
 */
public class TrieTests extends TestCase {
    private static final Logger LOGGER = Logger.getLogger(TrieTests.class.getName());
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final static int DEFAULT_COLUMN = 5;
    private final static int ADDRESS_COLUMN = 2;
    private final static int COMMENT_COLUMN = 8;
    private final static int REGION_COLUMN = 1;
    private final static int ZIPFAN_COLUMN = 0;
    private static final short DEFAULT_TRIE_LIMIT = 2;
    private static final boolean DEFAULT_SELF_GROW = true;

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @Test
    public void testTernaryTrie() throws Exception {
        TernaryMemoryTrie<SequentialAccessor> tmt = new TernaryMemoryTrie<SequentialAccessor>(DEFAULT_TRIE_LIMIT,
                DEFAULT_SELF_GROW);
        BufferedReader br = new BufferedReader(new FileReader("data/tpch0.001/orders.tbl"));
        String line = null;
        while (null != (line = br.readLine())) {
            String[] fields = line.split("\\|");
            UTF8StringPointable key = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
            String strD = fields[COMMENT_COLUMN];
            byte[] buf = HistogramUtils.toUTF8Byte(strD/*.toCharArray()*/, 0);
            key.set(buf, 0, UTF8StringUtil.getUTFLength(buf, 0));
            StringBuilder sb = new StringBuilder();
            UTF8StringUtil.toString(sb, key.getByteArray(), 0);
            SequentialAccessor sa = new SequentialAccessor(sb.toString());
            tmt.insert(sa, 0);
        }
        tmt.serialize(DEFAULT_TRIE_LIMIT);
        int count = tmt.getTotal();
        LOGGER.info("Total: " + count);
        //SequentialAccessor sa = null;
        Entry<String, Integer> si = null;
        int total = 0;
        String quantileOut = "";
        while ((si = tmt.next(true)) != null) {
            quantileOut += (si.getKey() + ", " + si.getValue() + "\n");
            total += si.getValue();
        }
        LOGGER.info(quantileOut);
        LOGGER.info("post total: " + total);
        /*List<Entry<UTF8StringPointable, Integer>> quantiles = tmt.generate();
        for (int i = 0; i < quantiles.size(); i++) {
            StringBuilder sb = new StringBuilder();
            UTF8StringPointable.toString(sb, quantiles.get(i).getKey().getByteArray(), 0);
            System.out.print("<" + sb.toString() + ", " + quantiles.get(i).getValue() + ">\n");
        }
        br.close();
        tmt.countReset();
        System.out.println("Verification");
        br = new BufferedReader(new FileReader("/Users/michael/Desktop/tpch_2_16_1/dbgen/orders.tbl"));
        line = null;
        while (null != (line = br.readLine())) {
            String[] fields = line.split("\\|");
            UTF8StringPointable key = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
            String strD = fields[COMMENT_COLUMN];
            byte[] buf = SampleUtils.toUTF8Byte(strD.toCharArray(), 0);
            key.set(buf, 0, UTF8StringPointable.getUTFLength(buf, 0));
            dth.countItem(key);
        }
        quantiles = dth.generate();
        for (int i = 0; i < quantiles.size(); i++) {
            StringBuilder sb = new StringBuilder();
            UTF8StringPointable.toString(sb, quantiles.get(i).getKey().getByteArray(), 0);
            System.out.print("<" + i + ", " + sb.toString() + ", " + quantiles.get(i).getValue() + ">\n");
        }*/
    }
}
