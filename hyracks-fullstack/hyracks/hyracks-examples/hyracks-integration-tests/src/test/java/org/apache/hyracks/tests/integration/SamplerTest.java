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
package org.apache.hyracks.tests.integration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import junit.framework.Assert;

import org.apache.hyracks.dataflow.std.parallel.sampler.ISampler;
import org.apache.hyracks.dataflow.std.parallel.sampler.ReservoirSampler;
import org.junit.Test;

public class SamplerTest {
    private static final Logger LOGGER = Logger.getLogger(SamplerTest.class.getName());

    private static final String filePath = "data/skew/zipfan.tbl";

    private final static int ZIPFAN_COLUMN = 0;

    private final static int SAMPLE_COUNT = 4000;

    private final static double ERROR_BOUND = 0.2;

    private final static int PARTITION_COUNT = 8;

    private int totalCount = 0;

    private final static Map<Integer, Double> randDatum = new HashMap<Integer, Double>();

    @Test
    public void testSampler() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line = null;
        while (null != (line = br.readLine())) {
            String[] fields = line.split("\t");
            String strD = fields[ZIPFAN_COLUMN];
            randDatum.put((int) (Math.random() * 1000000000), Double.parseDouble(strD));
            totalCount++;
        }
        br.close();

        ISampler<Double> sampler = new ReservoirSampler<Double>(SAMPLE_COUNT, true);

        Iterator<Entry<Integer, Double>> iter = randDatum.entrySet().iterator();
        while (iter.hasNext())
            sampler.sample(iter.next().getValue());

        Iterator<Double> sampleed = sampler.getSamples().iterator();
        LOGGER.info("Total sampled: " + sampler.getSize());
        List<Double> rangeMap = new ArrayList<Double>();
        int[] count = new int[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++)
            count[i] = 0;
        int current = 0;
        while (sampleed.hasNext()) {
            ++current;
            if (current % (SAMPLE_COUNT / PARTITION_COUNT) == 0)
                rangeMap.add(sampleed.next());
            else
                sampleed.next();
        }
        LOGGER.info("rangeMap: " + rangeMap.size() + " actual: " + current);
        if (rangeMap.size() == PARTITION_COUNT)
            rangeMap.remove(rangeMap.size() - 1);

        iter = randDatum.entrySet().iterator();

        while (iter.hasNext()) {
            double value = iter.next().getValue();
            boolean found = false;
            for (int i = 0; i < rangeMap.size(); i++) {
                if (rangeMap.get(i) > value) {
                    count[i]++;
                    found = true;
                    break;
                }
            }
            if (!found)
                count[count.length - 1]++;
        }

        int cMax = 0;
        for (int i = 0; i < count.length - 1; i++) {
            LOGGER.info(rangeMap.get(i) + " <-> " + count[i]);
        }
        LOGGER.info("INF <-> " + count[count.length - 1]);
        for (int i = 0; i < count.length; i++) {
            if (cMax < count[i])
                cMax = count[i];
        }
        Assert.assertEquals(0,
                (int) ((cMax - (totalCount / PARTITION_COUNT)) / ((double) totalCount / PARTITION_COUNT * ERROR_BOUND)));
    }
}
