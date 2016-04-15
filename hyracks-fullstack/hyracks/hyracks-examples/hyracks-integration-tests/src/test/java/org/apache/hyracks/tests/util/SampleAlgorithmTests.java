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
package org.apache.hyracks.tests.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import junit.framework.TestCase;

import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.std.parallel.IHistogram;
import org.apache.hyracks.dataflow.std.parallel.IIterativeHistogram;
import org.apache.hyracks.dataflow.std.parallel.histogram.structures.DTStreamingHistogram;
import org.apache.hyracks.dataflow.std.parallel.histogram.structures.TernaryIterativeHistogram;
import org.apache.hyracks.dataflow.std.parallel.util.DualSerialEntry;
import org.apache.hyracks.dataflow.std.parallel.util.HistogramUtils;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author michael
 */
public class SampleAlgorithmTests extends TestCase {
    private static final Logger LOGGER = Logger.getLogger(SampleAlgorithmTests.class.getName());
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final static int DEFAULT_COLUMN = 5;
    //private final static int ADDRESS_COLUMN = 2;
    private final static int COMMENT_COLUMN = 15;
    private final static int ZIPFAN_COLUMN = 0;
    private/*final static*/int PARTITION_CARD = 7;
    private final static double precision = 0.01;
    private final static boolean fixPointable = false;
    private final static boolean printQuantiles = false;
    private final static boolean deeper = true;
    private final int sampleJump = 1;
    private final static boolean randomSample = true;
    private final static boolean coveredTest = false;

    private final static String filePath = "data/tpch0.001/lineitem.tbl";

    //private final static String filePath = "/Users/michael/Desktop/tpch_2_16_1/dbgen/lineitem.tbl";

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @SuppressWarnings("unused")
    @Test
    public void testIterativeTernaryHitogram() throws Exception {
        IHistogram<UTF8StringPointable> tih = new TernaryIterativeHistogram(PARTITION_CARD, precision, fixPointable,
                deeper);
        tih.initialize();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line = null;
        int total = 0;
        int round = 0;

        //Sampling histogram
        long begin = System.currentTimeMillis();
        BitSet sampledBits = new BitSet();
        long start = System.currentTimeMillis();
        while (null != (line = br.readLine())) {
            if (randomSample) {
                if (sampleJump > 1 && Math.round(Math.random() * (double) sampleJump) != 4) {
                    total++;
                    continue;
                } else
                    total++;
            } else {
                if (total++ % sampleJump != 0) {
                    continue;
                }
            }
            sampledBits.set(total);
            String[] fields = line.split("\\|");
            UTF8StringPointable key = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
            String strD = fields[COMMENT_COLUMN];
            byte[] buf = HistogramUtils.toUTF8Byte(strD/*.toCharArray()*/, 0);
            key.set(buf, 0, UTF8StringUtil.getUTFLength(buf, 0));
            tih.addItem(key);
        }
        LOGGER.info("Round" + round + " elipse: " + (System.currentTimeMillis() - start) + " ");
        boolean need = ((IIterativeHistogram<UTF8StringPointable>) tih).needIteration();
        while (need) {
            start = System.currentTimeMillis();
            int current = 0;
            ((IIterativeHistogram<UTF8StringPointable>) tih).disperse();
            round++;
            LOGGER.info("Round" + round + " elipse: " + (System.currentTimeMillis() - start) + " ");
            need = ((IIterativeHistogram<UTF8StringPointable>) tih).needIteration();
        }

        //Sequential Merge
        List<Entry<UTF8StringPointable, Integer>> generated = tih.generate(true);
        String quantileOut = "";

        quantileOut = "";
        for (int i = 0; i < generated.size(); i++) {
            StringBuilder sb = new StringBuilder();
            UTF8StringUtil.toString(sb, generated.get(i).getKey().getByteArray(), 0);
            quantileOut += ("<" + sb.toString() + ", " + generated.get(i).getValue() + ">\n");
        }
        LOGGER.info(quantileOut);
        quantileOut = "";
        long end = System.currentTimeMillis();
        LOGGER.info("fixed before clipse: " + (end - begin));
        if (fixPointable) {
            List<Entry<UTF8StringPointable, Integer>> fixed = ((IIterativeHistogram<UTF8StringPointable>) tih)
                    .getFixPointable();
            for (int i = 0; i < fixed.size(); i++) {
                StringBuilder sb = new StringBuilder();
                UTF8StringUtil.toString(sb, fixed.get(i).getKey().getByteArray(), 0);
                quantileOut += ("<" + sb.toString() + ", " + fixed.get(i).getValue() + ">\n");
            }
        }
        LOGGER.info(quantileOut);
        quantileOut = "";
        //Verification
        LOGGER.info("Verification:");

        List<String> ticks = new ArrayList<String>();
        List<Integer> counts = new ArrayList<Integer>();
        for (int i = 0; i < generated.size(); i++) {
            StringBuilder lastString = new StringBuilder();
            UTF8StringUtil.toString(lastString, generated.get(i).getKey().getByteArray(), 0);
            ticks.add(lastString.toString());
            counts.add(0);
        }

        br.close();
        br = new BufferedReader(new FileReader(filePath));
        Map<Integer, Integer> strLengthMap = new HashMap<Integer, Integer>();
        if (counts.size() > 0) {
            while (null != (line = br.readLine())) {
                String[] fields = line.split("\\|");
                String strD = fields[COMMENT_COLUMN];
                if (coveredTest) {
                    Integer len = strD.length();
                    if (strLengthMap.containsKey(len))
                        strLengthMap.put(len, strLengthMap.get(len) + 1);
                    else
                        strLengthMap.put(len, 1);
                }
                boolean isLast = true;
                for (int i = 0; i < ticks.size() - 1; i++) {
                    if (ticks.get(i).compareTo(strD) >= 0) {
                        counts.set(i, counts.get(i) + 1);
                        isLast = false;
                        break;
                    }
                }
                if (isLast)
                    counts.set(counts.size() - 1, counts.get(counts.size() - 1) + 1);
            }
        }

        for (int i = 0; i < ticks.size(); i++) {
            quantileOut += ("[" + ticks.get(i) + ", " + counts.get(i) + "]\n");
        }

        LOGGER.info(quantileOut);
        for (Entry<Integer, Integer> e : strLengthMap.entrySet())
            LOGGER.info("length: " + e.getKey() + " has: " + e.getValue());

        br.close();
    }

    @SuppressWarnings("unused")
    @Test
    public void testTernaryHistogram() throws Exception {
        IHistogram<UTF8StringPointable> tih = new TernaryIterativeHistogram(PARTITION_CARD, precision, fixPointable,
                false);
        tih.initialize();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line = null;
        int total = 0;
        int round = 1;

        //Sampling histogram
        long begin = System.currentTimeMillis();
        BitSet sampledBits = new BitSet();
        long start = System.currentTimeMillis();
        while (null != (line = br.readLine())) {
            if (randomSample) {
                if (sampleJump > 1 && Math.round(Math.random() * (double) sampleJump) != 4) {
                    total++;
                    continue;
                } else
                    total++;
            } else {
                if (total++ % sampleJump != 0) {
                    continue;
                }
            }
            sampledBits.set(total);
            String[] fields = line.split("\\|");
            UTF8StringPointable key = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
            String strD = fields[COMMENT_COLUMN];
            byte[] buf = HistogramUtils.toUTF8Byte(strD/*.toCharArray()*/, 0);
            key.set(buf, 0, UTF8StringUtil.getUTFLength(buf, 0));
            tih.addItem(key);
        }
        LOGGER.info("Round" + round + " elipse: " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        boolean need = ((IIterativeHistogram<UTF8StringPointable>) tih).needIteration();
        while (need) {
            start = System.currentTimeMillis();
            int current = 0;
            tih.initialize();
            br.close();
            br = new BufferedReader(new FileReader(filePath));
            while (null != (line = br.readLine())) {
                if (!sampledBits.get(current++))
                    continue;
                String[] fields = line.split("\\|");
                UTF8StringPointable key = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
                String strD = fields[COMMENT_COLUMN];
                byte[] buf = HistogramUtils.toUTF8Byte(strD/*.toCharArray()*/, 0);
                key.set(buf, 0, UTF8StringUtil.getUTFLength(buf, 0));
                tih.addItem(key);
            }
            round++;
            need = ((IIterativeHistogram<UTF8StringPointable>) tih).needIteration();
            LOGGER.info("Round" + round + " elipse: " + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
        }

        //Sequential Merge
        String quantileOut = "";
        List<Entry<UTF8StringPointable, Integer>> generated = tih.generate(true);

        quantileOut = "";
        for (int i = 0; i < generated.size(); i++) {
            StringBuilder sb = new StringBuilder();
            UTF8StringUtil.toString(sb, generated.get(i).getKey().getByteArray(), 0);
            quantileOut += ("<" + sb.toString() + ", " + generated.get(i).getValue() + ">\n");
        }
        LOGGER.info(quantileOut);

        quantileOut = "";
        long end = System.currentTimeMillis();
        LOGGER.info("fixed before clipse: " + (end - begin));
        if (fixPointable) {
            List<Entry<UTF8StringPointable, Integer>> fixed = ((IIterativeHistogram<UTF8StringPointable>) tih)
                    .getFixPointable();
            for (int i = 0; i < fixed.size(); i++) {
                StringBuilder sb = new StringBuilder();
                UTF8StringUtil.toString(sb, fixed.get(i).getKey().getByteArray(), 0);
                quantileOut += ("<" + sb.toString() + ", " + fixed.get(i).getValue() + ">\n");
            }
        }
        LOGGER.info(quantileOut);

        //Verification
        LOGGER.info("Verification");

        List<String> ticks = new ArrayList<String>();
        List<Integer> counts = new ArrayList<Integer>();
        for (int i = 0; i < generated.size(); i++) {
            StringBuilder lastString = new StringBuilder();
            UTF8StringUtil.toString(lastString, generated.get(i).getKey().getByteArray(), 0);
            ticks.add(lastString.toString());
            counts.add(0);
        }

        br.close();
        br = new BufferedReader(new FileReader(filePath));
        Map<Integer, Integer> strLengthMap = new HashMap<Integer, Integer>();
        if (counts.size() > 0) {
            while (null != (line = br.readLine())) {
                String[] fields = line.split("\\|");
                String strD = fields[COMMENT_COLUMN];
                if (coveredTest) {
                    Integer len = strD.length();
                    if (strLengthMap.containsKey(len))
                        strLengthMap.put(len, strLengthMap.get(len) + 1);
                    else
                        strLengthMap.put(len, 1);
                }
                boolean isLast = true;
                for (int i = 0; i < ticks.size() - 1; i++) {
                    if (ticks.get(i).compareTo(strD) >= 0) {
                        counts.set(i, counts.get(i) + 1);
                        isLast = false;
                        break;
                    }
                }
                if (isLast)
                    counts.set(counts.size() - 1, counts.get(counts.size() - 1) + 1);
            }
        }

        quantileOut = "";
        for (int i = 0; i < ticks.size(); i++) {
            quantileOut += ("[" + ticks.get(i) + ", " + counts.get(i) + "]\n");
        }
        LOGGER.info(quantileOut);

        quantileOut = "";
        for (Entry<Integer, Integer> e : strLengthMap.entrySet())
            quantileOut += ("length: " + e.getKey() + " has: " + e.getValue());
        LOGGER.info(quantileOut);

        br.close();
    }

    //    @Test
    //    public void testAnsiHistogram() throws Exception {
    //        DTStreamingHistogram<UTF8StringPointable> dth = new DTStreamingHistogram<UTF8StringPointable>(
    //                IHistogram.FieldType.UTF8);
    //        dth.initialize();
    //        dth.allocate(PARTITION_CARD);
    //        BufferedReader br = new BufferedReader(new FileReader("/Users/michael/Desktop/tpch_2_16_1/dbgen/lineitem.tbl"));
    //        String line = null;
    //        while (null != (line = br.readLine())) {
    //            String[] fields = line.split("\\|");
    //            UTF8StringPointable key = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    //            String strD = fields[COMMENT_COLUMN];
    //            byte[] buf = SampleUtils.toUTF8Byte(strD.toCharArray(), 0);
    //            key.set(buf, 0, UTF8StringPointable.getUTFLength(buf, 0));
    //            dth.addItem(key);
    //        }
    //        List<Entry<UTF8StringPointable, Integer>> quantiles = dth.generate();
    //        for (int i = 0; i < quantiles.size(); i++) {
    //            StringBuilder sb = new StringBuilder();
    //            UTF8StringPointable.toString(sb, quantiles.get(i).getKey().getByteArray(), 0);
    //            System.out.print("<" + sb.toString() + ", " + quantiles.get(i).getValue() + ">\n");
    //        }
    //        br.close();
    //        dth.countReset();
    //        System.out.println("Verification");
    //        br = new BufferedReader(new FileReader("/Users/michael/Desktop/tpch_2_16_1/dbgen/lineitem.tbl"));
    //        line = null;
    //        while (null != (line = br.readLine())) {
    //            String[] fields = line.split("\\|");
    //            UTF8StringPointable key = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    //            String strD = fields[COMMENT_COLUMN];
    //            byte[] buf = SampleUtils.toUTF8Byte(strD.toCharArray(), 0);
    //            key.set(buf, 0, UTF8StringPointable.getUTFLength(buf, 0));
    //            dth.countItem(key);
    //        }
    //        quantiles = dth.generate();
    //        for (int i = 0; i < quantiles.size(); i++) {
    //            StringBuilder sb = new StringBuilder();
    //            UTF8StringPointable.toString(sb, quantiles.get(i).getKey().getByteArray(), 0);
    //            System.out.print("<" + i + ", " + sb.toString() + ", " + quantiles.get(i).getValue() + ">\n");
    //        }
    //    }
    //    @Test
    //    public void testRandomHistogram() throws Exception {
    //        DTStreamingHistogram<DoublePointable> dth = new DTStreamingHistogram<DoublePointable>(
    //                IHistogram.FieldType.DOUBLE);
    //        dth.initialize();
    //        dth.allocate(PARTITION_CARD);
    //        BufferedReader br = new BufferedReader(new FileReader("/Users/michael/Desktop/tpch_2_16_1/dbgen/customer.tbl"));
    //        String line = null;
    //        while (null != (line = br.readLine())) {
    //            String[] fields = line.split("\\|");
    //            DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //            byte[] buf = new byte[DoublePointable.TYPE_TRAITS.getFixedLength()];
    //            key.set(buf, 0, DoublePointable.TYPE_TRAITS.getFixedLength());
    //            String strD = fields[DEFAULT_COLUMN];
    //            double d = Double.parseDouble(strD);
    //            key.setDouble(d);
    //            dth.addItem(key);
    //        }
    //        List<Entry<DoublePointable, Integer>> quantiles = dth.generate();
    //        for (int i = 0; i < quantiles.size(); i++) {
    //            System.out.print("<" + quantiles.get(i).getKey().getDouble() + ", " + quantiles.get(i).getValue() + ">\n");
    //            //            LOGGER.warning("<" + DoublePointable.getDouble(quantiles.get(i).getKey(), 0) + ", "
    //            //                    + quantiles.get(i).getValue() + "\n");
    //        }
    //        br.close();
    //        dth.countReset();
    //        System.out.println("Verification");
    //        br = new BufferedReader(new FileReader("/Users/michael/Desktop/tpch_2_16_1/dbgen/customer.tbl"));
    //        line = null;
    //        while (null != (line = br.readLine())) {
    //            String[] fields = line.split("\\|");
    //            DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //            byte[] buf = new byte[DoublePointable.TYPE_TRAITS.getFixedLength()];
    //            key.set(buf, 0, DoublePointable.TYPE_TRAITS.getFixedLength());
    //            String strD = fields[DEFAULT_COLUMN];
    //            double d = Double.parseDouble(strD);
    //            key.setDouble(d);
    //            dth.countItem(key);
    //        }
    //        quantiles = dth.generate();
    //        for (int i = 0; i < quantiles.size(); i++) {
    //            System.out.print("<" + i + ", " + quantiles.get(i).getKey().getDouble() + ", "
    //                    + quantiles.get(i).getValue() + ">\n");
    //            //            LOGGER.warning("<" + DoublePointable.getDouble(quantiles.get(i).getKey(), 0) + ", "
    //            //                    + quantiles.get(i).getValue() + "\n");
    //        }
    //    }
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static <K extends Comparable<? super K>, V> Map<K, V> sortByKey(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getKey()).compareTo(o2.getKey());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @Test
    public void testZipfanRandom() throws Exception {
        String zipFanFilePath = "data/skew/zipfan.tbl";
        /*for (int part = 20; part < 1025; part *= 200) {
            for (int scale = 1; scale < 3; scale++) {
                PARTITION_CARD = part;*/
        DTStreamingHistogram<DoublePointable> dth = new DTStreamingHistogram<DoublePointable>(
                IHistogram.FieldType.DOUBLE, true);
        dth.initialize();
        dth.allocate(PARTITION_CARD, 16, true);
        Map<Integer, DoublePointable> randString = new HashMap<Integer, DoublePointable>();
        BufferedReader br = new BufferedReader(new FileReader(zipFanFilePath));
        String line = null;
        /*IBinaryHashFunction hashFunction = new PointableBinaryHashFunctionFactory(DoublePointable.FACTORY)
                .createBinaryHashFunction();*/
        while (null != (line = br.readLine())) {
            String[] fields = line.split("\t");
            DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
            byte[] buf = new byte[Double.SIZE / Byte.SIZE];
            key.set(buf, 0, Double.SIZE / Byte.SIZE);
            String strD = fields[ZIPFAN_COLUMN];
            double d = Double.parseDouble(strD);
            key.setDouble(d);
            //            randString.put(hashFunction.hash(key, 0, key.length), key);
            randString.put((int) (Math.random() * 1000000000), key);
        }
        randString = sortByKey(randString);

        long begin = System.currentTimeMillis();
        for (Entry<Integer, DoublePointable> entry : randString.entrySet())
            dth.addItem(entry.getValue());
        List<Entry<DoublePointable, Integer>> quantiles = dth.generate(true);
        String quantileOut = "";
        for (int i = 0; i < quantiles.size(); i++) {
            quantileOut += ("<" + quantiles.get(i).getKey().getDouble() + ", " + quantiles.get(i).getValue() + ">\n");
        }
        LOGGER.info(quantileOut);
        br.close();
        dth.countReset();
        long end = System.currentTimeMillis();
        LOGGER.info("Eclipse: " + (end - begin));
        LOGGER.info("Verification");
        br = new BufferedReader(new FileReader(zipFanFilePath));
        line = null;
        while (null != (line = br.readLine())) {
            String[] fields = line.split("\t");
            DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
            byte[] buf = new byte[Double.SIZE / Byte.SIZE];
            key.set(buf, 0, Double.SIZE / Byte.SIZE);
            String strD = fields[ZIPFAN_COLUMN];
            double d = Double.parseDouble(strD);
            key.setDouble(d);
            dth.countItem(key);
        }
        quantiles = dth.generate(true);
        int maximal = 0;
        int minimal = Integer.MAX_VALUE;
        int total = 0;
        quantileOut = "";
        for (int i = 0; i < quantiles.size(); i++) {
            quantileOut += ("<" + i + ", " + quantiles.get(i).getKey().getDouble() + ", " + quantiles.get(i).getValue() + ">\n");
            int current = quantiles.get(i).getValue();
            if (current > maximal)
                maximal = current;
            if (current < minimal)
                minimal = current;
            total += current;
        }
        LOGGER.info(quantileOut);
    }

    //    public static void main(String[] args) throws IOException {
    //        if (args.length != 4) {
    //            System.out.println("Command format: command part scale equalHeight file");
    //        }
    //        int part = Integer.parseInt(args[0]);
    //        int scale = Integer.parseInt(args[1]);
    //        boolean equalHeight = Boolean.parseBoolean(args[2]);
    //        String zipFanFilePath = args[3];
    //
    //        part = 40;
    //        scale = 8;
    //        equalHeight = true;
    //        zipFanFilePath = "/Users/michael/chenli/whu/algorithm/skew.txt";
    //        /*for (int part = 20; part < 1025; part *= 200) {
    //            for (int scale = 1; scale < 3; scale++) {*/
    //        Map<Integer, DoublePointable> randString = new HashMap<Integer, DoublePointable>();
    //        BufferedReader br = new BufferedReader(new FileReader(zipFanFilePath));
    //        String line = null;
    //        /*IBinaryHashFunction hashFunction = new PointableBinaryHashFunctionFactory(DoublePointable.FACTORY)
    //                .createBinaryHashFunction();*/
    //        while (null != (line = br.readLine())) {
    //            String[] fields = line.split("\t");
    //            DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //            byte[] buf = new byte[Double.SIZE / Byte.SIZE];
    //            key.set(buf, 0, Double.SIZE / Byte.SIZE);
    //            String strD = fields[ZIPFAN_COLUMN];
    //            double d = Double.parseDouble(strD);
    //            key.setDouble(d);
    //            //            randString.put(hashFunction.hash(key, 0, key.length), key);
    //            randString.put((int) (Math.random() * 1000000000), key);
    //        }
    //        randString = sortByKey(randString);
    //        {
    //            DTStreamingHistogram<DoublePointable> dth = new DTStreamingHistogram<DoublePointable>(
    //                    IHistogram.FieldType.DOUBLE, true);
    //            dth.initialize();
    //            dth.allocate(part, scale, equalHeight);
    //            System.out.println("begin sample");
    //            long begin = System.currentTimeMillis();
    //            int ii = 0;
    //            for (Entry<Integer, DoublePointable> entry : randString.entrySet()) {
    //                dth.addItem(entry.getValue());
    //            }
    //            List<Entry<DoublePointable, Integer>> quantiles = dth.generate();
    //            for (int i = 0; i < quantiles.size(); i++) {
    //                System.out.print("<" + quantiles.get(i).getKey().getDouble() + ", " + quantiles.get(i).getValue()
    //                        + ">\n");
    //            }
    //            br.close();
    //            dth.countReset();
    //            long end = System.currentTimeMillis();
    //            /*System.out.println("Verification");*/
    //            br = new BufferedReader(new FileReader(zipFanFilePath));
    //            line = null;
    //            while (null != (line = br.readLine())) {
    //                String[] fields = line.split("\t");
    //                DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //                byte[] buf = new byte[Double.SIZE / Byte.SIZE];
    //                key.set(buf, 0, Double.SIZE / Byte.SIZE);
    //                String strD = fields[ZIPFAN_COLUMN];
    //                double d = Double.parseDouble(strD);
    //                key.setDouble(d);
    //                dth.countItem(key);
    //            }
    //            quantiles = dth.generate();
    //            int maximal = 0;
    //            int minimal = Integer.MAX_VALUE;
    //            int total = 0;
    //            for (int i = 0; i < quantiles.size(); i++) {
    //                /*System.out.print("<" + i + ", " + quantiles.get(i).getKey().getDouble() + ", "
    //                        + quantiles.get(i).getValue() + ">\n");*/
    //                int current = quantiles.get(i).getValue();
    //                if (current > maximal)
    //                    maximal = current;
    //                if (current < minimal)
    //                    minimal = current;
    //                total += current;
    //            }
    //            System.out.println(quantiles.size() + "\t" + part + "\t" + scale + "\t" + (end - begin) + "\t" + maximal
    //                    + "\t" + minimal + "\t" + total / part + "\t" + maximal * (double) part / total + "\t"
    //                    + dth.updateHeap);
    //        }
    //        {
    //            DTStreamingHistogram<DoublePointable> dth = new DTStreamingHistogram<DoublePointable>(
    //                    IHistogram.FieldType.DOUBLE, false);
    //            dth.initialize();
    //            dth.allocate(part, scale, equalHeight);
    //            System.out.println("begin sample");
    //            long begin = System.currentTimeMillis();
    //            for (Entry<Integer, DoublePointable> entry : randString.entrySet())
    //                dth.addItem(entry.getValue());
    //            List<Entry<DoublePointable, Integer>> quantiles = dth.generate();
    //            for (int i = 0; i < quantiles.size(); i++) {
    //                System.out.print("<" + quantiles.get(i).getKey().getDouble() + ", " + quantiles.get(i).getValue()
    //                        + ">\n");
    //            }
    //            br.close();
    //            dth.countReset();
    //            long end = System.currentTimeMillis();
    //            /*System.out.println("Verification");*/
    //            br = new BufferedReader(new FileReader(zipFanFilePath));
    //            line = null;
    //            while (null != (line = br.readLine())) {
    //                String[] fields = line.split("\t");
    //                DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //                byte[] buf = new byte[Double.SIZE / Byte.SIZE];
    //                key.set(buf, 0, Double.SIZE / Byte.SIZE);
    //                String strD = fields[ZIPFAN_COLUMN];
    //                double d = Double.parseDouble(strD);
    //                key.setDouble(d);
    //                dth.countItem(key);
    //            }
    //            quantiles = dth.generate();
    //            int maximal = 0;
    //            int minimal = Integer.MAX_VALUE;
    //            int total = 0;
    //            for (int i = 0; i < quantiles.size(); i++) {
    //                /*System.out.print("<" + i + ", " + quantiles.get(i).getKey().getDouble() + ", "
    //                        + quantiles.get(i).getValue() + ">\n");*/
    //                int current = quantiles.get(i).getValue();
    //                if (current > maximal)
    //                    maximal = current;
    //                if (current < minimal)
    //                    minimal = current;
    //                total += current;
    //            }
    //            System.out.println(quantiles.size() + "\t" + part + "\t" + scale + "\t" + (end - begin) + "\t" + maximal
    //                    + "\t" + minimal + "\t" + total / part + "\t" + maximal * (double) part / total + "\t"
    //                    + dth.updateHeap);
    //        }
    //        {
    //            DTStreamingHistogram<DoublePointable> dth = new DTStreamingHistogram<DoublePointable>(
    //                    IHistogram.FieldType.DOUBLE, false);
    //            dth.initialize();
    //            dth.allocate(part, scale, equalHeight);
    //            System.out.println("begin sample");
    //            long begin = System.currentTimeMillis();
    //            for (Entry<Integer, DoublePointable> entry : randString.entrySet())
    //                dth.addItem(entry.getValue());
    //            List<Entry<DoublePointable, Integer>> quantiles = dth.generate();
    //            for (int i = 0; i < quantiles.size(); i++) {
    //                System.out.print("<" + quantiles.get(i).getKey().getDouble() + ", " + quantiles.get(i).getValue()
    //                        + ">\n");
    //            }
    //            br.close();
    //            dth.countReset();
    //            long end = System.currentTimeMillis();
    //            /*System.out.println("Verification");*/
    //            br = new BufferedReader(new FileReader(zipFanFilePath));
    //            line = null;
    //            while (null != (line = br.readLine())) {
    //                String[] fields = line.split("\t");
    //                DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //                byte[] buf = new byte[Double.SIZE / Byte.SIZE];
    //                key.set(buf, 0, Double.SIZE / Byte.SIZE);
    //                String strD = fields[ZIPFAN_COLUMN];
    //                double d = Double.parseDouble(strD);
    //                key.setDouble(d);
    //                dth.countItem(key);
    //            }
    //            quantiles = dth.generate();
    //            int maximal = 0;
    //            int minimal = Integer.MAX_VALUE;
    //            int total = 0;
    //            for (int i = 0; i < quantiles.size(); i++) {
    //                /*System.out.print("<" + i + ", " + quantiles.get(i).getKey().getDouble() + ", "
    //                        + quantiles.get(i).getValue() + ">\n");*/
    //                int current = quantiles.get(i).getValue();
    //                if (current > maximal)
    //                    maximal = current;
    //                if (current < minimal)
    //                    minimal = current;
    //                total += current;
    //            }
    //            System.out.println(quantiles.size() + "\t" + part + "\t" + scale + "\t" + (end - begin) + "\t" + maximal
    //                    + "\t" + minimal + "\t" + total / part + "\t" + maximal * (double) part / total + "\t"
    //                    + dth.updateHeap);
    //        }
    //        /*}
    //        }*/
    //    }
    //
    //    @Test
    //    public static void testStream() throws IOException {
    //        int part = 3;
    //        int scale = 1;
    //        boolean equalHeight = true;
    //        String zipFanFilePath = "/Users/michael/chenli/whu/algorithm/stream_seq.txt";
    //        /*for (int part = 20; part < 1025; part *= 200) {
    //            for (int scale = 1; scale < 3; scale++) {*/
    //        BufferedReader br = new BufferedReader(new FileReader(zipFanFilePath));
    //        String line = null;
    //        /*IBinaryHashFunction hashFunction = new PointableBinaryHashFunctionFactory(DoublePointable.FACTORY)
    //                .createBinaryHashFunction();*/
    //        {
    //            DTStreamingHistogram<DoublePointable> dth = new DTStreamingHistogram<DoublePointable>(
    //                    IHistogram.FieldType.DOUBLE, true);
    //            dth.initialize();
    //            dth.allocate(part, scale, equalHeight);
    //            System.out.println("begin sample");
    //            long begin = System.currentTimeMillis();
    //            while (null != (line = br.readLine())) {
    //                String[] fields = line.split("\t");
    //                DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //                byte[] buf = new byte[Double.SIZE / Byte.SIZE];
    //                key.set(buf, 0, Double.SIZE / Byte.SIZE);
    //                String strD = fields[ZIPFAN_COLUMN];
    //                double d = Double.parseDouble(strD);
    //                key.setDouble(d);
    //                dth.addItem(key);
    //            }
    //            List<Entry<DoublePointable, Integer>> quantiles = dth.generate();
    //            /*for (int i = 0; i < quantiles.size(); i++) {
    //                System.out.print("<" + quantiles.get(i).getKey().getDouble() + ", " + quantiles.get(i).getValue()
    //                        + ">\n");
    //            }*/
    //            br.close();
    //            dth.countReset();
    //            long end = System.currentTimeMillis();
    //            /*System.out.println("Verification");*/
    //            br = new BufferedReader(new FileReader(zipFanFilePath));
    //            line = null;
    //            while (null != (line = br.readLine())) {
    //                String[] fields = line.split("\t");
    //                DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //                byte[] buf = new byte[Double.SIZE / Byte.SIZE];
    //                key.set(buf, 0, Double.SIZE / Byte.SIZE);
    //                String strD = fields[ZIPFAN_COLUMN];
    //                double d = Double.parseDouble(strD);
    //                key.setDouble(d);
    //                dth.countItem(key);
    //            }
    //            br.close();
    //            quantiles = dth.generate();
    //            int maximal = 0;
    //            int minimal = Integer.MAX_VALUE;
    //            int total = 0;
    //            for (int i = 0; i < quantiles.size(); i++) {
    //                System.out.print("<" + i + ", " + quantiles.get(i).getKey().getDouble() + ", "
    //                        + quantiles.get(i).getValue() + ">\n");
    //                int current = quantiles.get(i).getValue();
    //                if (current > maximal)
    //                    maximal = current;
    //                if (current < minimal)
    //                    minimal = current;
    //                total += current;
    //            }
    //            System.out.println(quantiles.size() + "\t" + part + "\t" + scale + "\t" + (end - begin) + "\t" + maximal
    //                    + "\t" + minimal + "\t" + total / part + "\t" + maximal * (double) part / total + "\t"
    //                    + dth.updateHeap);
    //        }
    //        {
    //            br = new BufferedReader(new FileReader(zipFanFilePath));
    //            DTStreamingHistogram<DoublePointable> dth = new DTStreamingHistogram<DoublePointable>(
    //                    IHistogram.FieldType.DOUBLE, false);
    //            dth.initialize();
    //            dth.allocate(part, scale, equalHeight);
    //            System.out.println("begin sample");
    //            long begin = System.currentTimeMillis();
    //            while (null != (line = br.readLine())) {
    //                String[] fields = line.split("\t");
    //                DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //                byte[] buf = new byte[Double.SIZE / Byte.SIZE];
    //                key.set(buf, 0, Double.SIZE / Byte.SIZE);
    //                String strD = fields[ZIPFAN_COLUMN];
    //                double d = Double.parseDouble(strD);
    //                key.setDouble(d);
    //                dth.addItem(key);
    //            }
    //            List<Entry<DoublePointable, Integer>> quantiles = dth.generate();
    //            /*for (int i = 0; i < quantiles.size(); i++) {
    //                System.out.print("<" + quantiles.get(i).getKey().getDouble() + ", " + quantiles.get(i).getValue()
    //                        + ">\n");
    //            }*/
    //            br.close();
    //            dth.countReset();
    //            long end = System.currentTimeMillis();
    //            /*System.out.println("Verification");*/
    //            br = new BufferedReader(new FileReader(zipFanFilePath));
    //            line = null;
    //            while (null != (line = br.readLine())) {
    //                String[] fields = line.split("\t");
    //                DoublePointable key = (DoublePointable) DoublePointable.FACTORY.createPointable();
    //                byte[] buf = new byte[Double.SIZE / Byte.SIZE];
    //                key.set(buf, 0, Double.SIZE / Byte.SIZE);
    //                String strD = fields[ZIPFAN_COLUMN];
    //                double d = Double.parseDouble(strD);
    //                key.setDouble(d);
    //                dth.countItem(key);
    //            }
    //            br.close();
    //            quantiles = dth.generate();
    //            int maximal = 0;
    //            int minimal = Integer.MAX_VALUE;
    //            int total = 0;
    //            for (int i = 0; i < quantiles.size(); i++) {
    //                System.out.print("<" + i + ", " + quantiles.get(i).getKey().getDouble() + ", "
    //                        + quantiles.get(i).getValue() + ">\n");
    //                int current = quantiles.get(i).getValue();
    //                if (current > maximal)
    //                    maximal = current;
    //                if (current < minimal)
    //                    minimal = current;
    //                total += current;
    //            }
    //            System.out.println(quantiles.size() + "\t" + part + "\t" + scale + "\t" + (end - begin) + "\t" + maximal
    //                    + "\t" + minimal + "\t" + total / part + "\t" + maximal * (double) part / total + "\t"
    //                    + dth.updateHeap);
    //        }
    //    }

    //    @Test
    //    public void testDecisionTreeZipfan() throws Exception {
    //        DecisionTreeHistogram dth = new DecisionTreeHistogram();
    //        dth.initialize();
    //        dth.allocate(300);
    //        BufferedReader br = new BufferedReader(new FileReader("/Users/michael/Desktop/zipfan.txt"));
    //        String line = null;
    //        while (null != (line = br.readLine())) {
    //            String[] fields = line.split("\t");
    //            byte[] key = new byte[Double.SIZE / Byte.SIZE];
    //            String strD = fields[ZIPFAN_COLUMN];
    //            double d = Double.parseDouble(strD);
    //            DoublePointable.setDouble(key, 0, d);
    //            dth.addItem(key);
    //        }
    //        List<Entry<byte[], Integer>> quantiles = dth.generate();
    //        for (int i = 0; i < quantiles.size(); i++) {
    //            System.out.print("<" + DoublePointable.getDouble(quantiles.get(i).getKey(), 0) + ", "
    //                    + quantiles.get(i).getValue() + ">\n");
    ////            LOGGER.warning("<" + DoublePointable.getDouble(quantiles.get(i).getKey(), 0) + ", "
    ////                    + quantiles.get(i).getValue() + "\n");
    //        }
    //        br.close();
    //        dth.countReset();
    //        System.out.println("Verification");
    //        br = new BufferedReader(new FileReader("/Users/michael/Desktop/zipfan.txt"));
    //        line = null;
    //        while (null != (line = br.readLine())) {
    //            String[] fields = line.split("\t");
    //            byte[] key = new byte[Double.SIZE / Byte.SIZE];
    //            String strD = fields[ZIPFAN_COLUMN];
    //            double d = Double.parseDouble(strD);
    //            DoublePointable.setDouble(key, 0, d);
    //            dth.countItem(key);
    //        }
    //        quantiles = dth.generate();
    //        for (int i = 0; i < quantiles.size(); i++) {
    //            System.out.print("<" + i + ", " + DoublePointable.getDouble(quantiles.get(i).getKey(), 0) + ", "
    //                    + quantiles.get(i).getValue() + ">\n");
    ////            LOGGER.warning("<" + DoublePointable.getDouble(quantiles.get(i).getKey(), 0) + ", "
    ////                    + quantiles.get(i).getValue() + "\n");
    //        }
    //    }

    @After
    public void cleanUpStreams() {
        System.setOut(null);
        System.setErr(null);
    }
}
